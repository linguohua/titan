package modules

import (
	"context"
	"errors"
	"github.com/google/uuid"
	"github.com/linguohua/titan/node/modules/dtypes"
	"github.com/linguohua/titan/node/modules/helpers"
	"github.com/linguohua/titan/node/repo"
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

func Datastore(lc fx.Lifecycle, mctx helpers.MetricsCtx, r repo.LockedRepo) (dtypes.MetadataDS, error) {
	ctx := helpers.LifecycleCtx(mctx, lc)
	mds, err := r.Datastore(ctx, "/metadata")
	if err != nil {
		return nil, err
	}

	return mds, nil
}
