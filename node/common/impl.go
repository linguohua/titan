package common

import (
	"context"
	"fmt"
	"github.com/linguohua/titan/node/modules/dtypes"
	"github.com/linguohua/titan/node/repo"
	"github.com/linguohua/titan/node/secret"
	"os"

	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/build"
	"github.com/linguohua/titan/journal/alerting"
	"github.com/linguohua/titan/node/handler"

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

	SessionCallBack dtypes.SessionCallbackFunc
}

type jwtPayload struct {
	Allow []auth.Permission
}

// MethodGroup: Auth

// NewCommonAPI New CommonAPI
func NewCommonAPI(lr repo.LockedRepo, callback dtypes.SessionCallbackFunc) (CommonAPI, error) {
	commApi := CommonAPI{
		SessionCallBack: callback,
	}

	sec, err := secret.APISecret(lr)
	if err != nil {
		return commApi, fmt.Errorf("NewLocalScheduleNode failed:%s", err.Error())
	}

	commApi.APISecret = sec
	return commApi, nil
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
	return nil, fmt.Errorf("not implement")
}

// Shutdown trigger graceful shutdown
func (a *CommonAPI) Shutdown(context.Context) error {
	a.ShutdownChan <- struct{}{}
	return nil
}

// Session returns a random UUID of api provider session
func (a *CommonAPI) Session(ctx context.Context) (uuid.UUID, error) {
	if a.SessionCallBack != nil {
		remoteAddr := handler.GetRemoteAddr(ctx)
		deviceID := handler.GetDeviceID(ctx)
		if deviceID != "" && remoteAddr != "" {
			a.SessionCallBack(deviceID, remoteAddr)
		}
	}

	return session, nil
}

func (a *CommonAPI) Closing(context.Context) (<-chan struct{}, error) {
	return make(chan struct{}), nil // relies on jsonrpc closing
}

func (a *CommonAPI) ShowLogFile(ctx context.Context) (*api.LogFile, error) {
	logFilePath := os.Getenv("GOLOG_FILE")
	if logFilePath == "" {
		return nil, fmt.Errorf("GOLOG_FILE not config, example: export GOLOG_FILE=/path/log")
	}
	info, err := os.Stat(logFilePath)
	if err != nil {
		return nil, err
	}

	return &api.LogFile{Name: info.Name(), Size: info.Size()}, nil
}

func (a *CommonAPI) DownloadLogFile(ctx context.Context) ([]byte, error) {
	logFilePath := os.Getenv("GOLOG_FILE")
	if logFilePath == "" {
		return nil, fmt.Errorf("GOLOG_FILE not config, example: export GOLOG_FILE=/path/log")
	}
	return os.ReadFile(logFilePath)
}

func (a *CommonAPI) DeleteLogFile(ctx context.Context) error {
	logFilePath := os.Getenv("GOLOG_FILE")
	if logFilePath == "" {
		return nil
	}

	os.WriteFile(logFilePath, []byte(""), 0o755)
	return nil
}
