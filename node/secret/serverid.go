package secret

import (
	"errors"

	"github.com/google/uuid"
	"github.com/linguohua/titan/node/repo"
	"github.com/linguohua/titan/node/types"

	"golang.org/x/xerrors"
)

const (
	ServerIDName     = "server-id"        //nolint:gosec
	KTServerIDSecret = "server-id-secret" //nolint:gosec
)

func ServerID(lr repo.LockedRepo) (string, error) {
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

	return string(key.PrivateKey), nil
}
