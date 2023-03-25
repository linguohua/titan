package modules

import (
	"context"
	"errors"

	"github.com/linguohua/titan/build"
	"github.com/linguohua/titan/lib/ulimit"

	"github.com/google/uuid"
	"github.com/linguohua/titan/node/modules/dtypes"
	"github.com/linguohua/titan/node/repo"
	"github.com/linguohua/titan/node/scheduler/assets"
	"github.com/linguohua/titan/node/scheduler/db"
	"github.com/linguohua/titan/node/types"
	"go.uber.org/fx"
	"golang.org/x/xerrors"
)

const (
	ServerIDName     = "server-id"        //nolint:gosec
	KTServerIDSecret = "server-id-secret" //nolint:gosec
)

func LockedRepo(lr repo.LockedRepo) func(lc fx.Lifecycle) repo.LockedRepo {
	return func(lc fx.Lifecycle) repo.LockedRepo {
		lc.Append(fx.Hook{
			OnStop: func(_ context.Context) error {
				return lr.Close()
			},
		})

		return lr
	}
}

// NewServerID create server id
func NewServerID(lr repo.LockedRepo) (dtypes.ServerID, error) {
	keystore, err := lr.KeyStore()
	if err != nil {
		return "", err
	}

	key, err := keystore.Get(ServerIDName)

	if errors.Is(err, types.ErrKeyInfoNotFound) {
		log.Warn("Generating new server id")

		uid := []byte(uuid.NewString())

		key = types.KeyInfo{
			Type:       KTServerIDSecret,
			PrivateKey: uid,
		}

		if err := keystore.Put(ServerIDName, key); err != nil {
			return "", xerrors.Errorf("writing server id: %w", err)
		}

		if err := lr.SetServerID(uid); err != nil {
			return "", err
		}
	} else if err != nil {
		return "", xerrors.Errorf("could not get server id: %w", err)
	}

	return dtypes.ServerID(key.PrivateKey), nil
}

func Datastore(db *db.SQLDB, serverID dtypes.ServerID) (dtypes.MetadataDS, error) {
	return assets.NewDatastore(db, serverID), nil
}

func CheckFdLimit() error {
	limit, _, err := ulimit.GetLimit()
	switch {
	case err == ulimit.ErrUnsupported:
		log.Errorw("checking file descriptor limit failed", "error", err)
	case err != nil:
		return xerrors.Errorf("checking fd limit: %w", err)
	default:
		if limit < build.EdgeFDLimit {
			return xerrors.Errorf("soft file descriptor limit (ulimit -n) too low, want %d, current %d", build.EdgeFDLimit, limit)
		}
	}
	return nil
}
