package common

import (
	"context"

	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/build"
	"github.com/linguohua/titan/journal/alerting"

	"github.com/filecoin-project/go-jsonrpc/auth"
	"github.com/gbrlsnchs/jwt/v3"
	"github.com/google/uuid"
	logging "github.com/ipfs/go-log/v2"
	"golang.org/x/xerrors"
)

var session = uuid.New()

type CommonAPI struct {
	Alerting     *alerting.Alerting
	APISecret    *jwt.HMACSHA
	ShutdownChan chan struct{}

	SessionCallBack func(string)
}

type jwtPayload struct {
	Allow []auth.Permission
}

// MethodGroup: Auth

// NewCommonAPI New CommonAPI
func NewCommonAPI(sessionCallBack func(string)) CommonAPI {
	return CommonAPI{SessionCallBack: sessionCallBack}
}

func (a *CommonAPI) AuthVerify(ctx context.Context, token string) ([]auth.Permission, error) {
	var payload jwtPayload
	if _, err := jwt.Verify([]byte(token), (*jwt.HMACSHA)(a.APISecret), &payload); err != nil {
		return nil, xerrors.Errorf("JWT Verification failed: %w", err)
	}

	return payload.Allow, nil
}

func (a *CommonAPI) AuthNew(ctx context.Context, perms []auth.Permission) ([]byte, error) {
	p := jwtPayload{
		Allow: perms, // TODO: consider checking validity
	}

	return jwt.Sign(&p, (*jwt.HMACSHA)(a.APISecret))
}

func (a *CommonAPI) LogList(context.Context) ([]string, error) {
	return logging.GetSubsystems(), nil
}

func (a *CommonAPI) LogSetLevel(ctx context.Context, subsystem, level string) error {
	return logging.SetLogLevel(subsystem, level)
}

func (a *CommonAPI) LogAlerts(ctx context.Context) ([]alerting.Alert, error) {
	return []alerting.Alert{}, nil
}

// Version provides information about API provider
func (a *CommonAPI) Version(context.Context) (api.APIVersion, error) {
	v, err := api.VersionForType(api.RunningNodeType)
	if err != nil {
		return api.APIVersion{}, err
	}

	return api.APIVersion{
		Version:    build.UserVersion(),
		APIVersion: v,
	}, nil
}

// Discover returns an OpenRPC document describing an RPC API.
func (a *CommonAPI) Discover(ctx context.Context) (api.OpenRPCDocument, error) {
	return nil, nil
}

// trigger graceful shutdown
func (a *CommonAPI) Shutdown(context.Context) error {
	a.ShutdownChan <- struct{}{}
	return nil
}

// Session returns a random UUID of api provider session
func (a *CommonAPI) Session(ctx context.Context, deviceID string) (uuid.UUID, error) {
	if a.SessionCallBack != nil {
		a.SessionCallBack(deviceID)
	}

	return session, nil
}

func (a *CommonAPI) Closing(context.Context) (<-chan struct{}, error) {
	return make(chan struct{}), nil // relies on jsonrpc closing
}
