package secret

import (
	"crypto/rand"
	"errors"
	"io"
	"io/ioutil"

	"github.com/filecoin-project/go-jsonrpc/auth"
	"github.com/gbrlsnchs/jwt/v3"
	logging "github.com/ipfs/go-log/v2"
	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/node/repo"
	"github.com/linguohua/titan/node/types"

	"golang.org/x/xerrors"
)

const (
	JWTSecretName   = "auth-jwt-private"
	KTJwtHmacSecret = "jwt-hmac-secret"
)

type JwtPayload struct {
	Allow []auth.Permission
}

var log = logging.Logger("jwt")

func APISecret(lr repo.LockedRepo) (*jwt.HMACSHA, error) {
	keystore, err := lr.KeyStore()
	if err != nil {
		return nil, err
	}

	key, err := keystore.Get(JWTSecretName)

	if errors.Is(err, types.ErrKeyInfoNotFound) {
		log.Warn("Generating new API secret")

		sk, err := ioutil.ReadAll(io.LimitReader(rand.Reader, 32))
		if err != nil {
			return nil, err
		}

		key = types.KeyInfo{
			Type:       KTJwtHmacSecret,
			PrivateKey: sk,
		}

		if err := keystore.Put(JWTSecretName, key); err != nil {
			return nil, xerrors.Errorf("writing API secret: %w", err)
		}

		// TODO: make this configurable
		p := JwtPayload{
			Allow: api.AllPermissions,
		}

		cliToken, err := jwt.Sign(&p, jwt.NewHS256(key.PrivateKey))
		if err != nil {
			return nil, err
		}

		if err := lr.SetAPIToken(cliToken); err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, xerrors.Errorf("could not get JWT Token: %w", err)
	}

	return jwt.NewHS256(key.PrivateKey), nil
}
