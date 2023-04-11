package common

import (
	"context"
	"fmt"
	"os"

	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/api/types"
	"github.com/linguohua/titan/build"
	"github.com/linguohua/titan/journal/alerting"
	"github.com/linguohua/titan/node/handler"
	"github.com/linguohua/titan/node/modules/dtypes"
	"github.com/linguohua/titan/node/repo"

	"github.com/filecoin-project/go-jsonrpc/auth"
	"github.com/gbrlsnchs/jwt/v3"
	"github.com/google/uuid"
	logging "github.com/ipfs/go-log/v2"
	"golang.org/x/xerrors"
)

var session = uuid.New()

type CommonAPI struct {
	Alerting        *alerting.Alerting
	APISecret       *jwt.HMACSHA
	ShutdownChan    chan struct{}
	SessionCallBack dtypes.SessionCallbackFunc
}

type jwtPayload struct {
	Allow []auth.Permission
}

type (
	PermissionWriteToken []byte
	PermissionAdminToken []byte
)

// SessionCallbackFunc will be called after node connection
type SessionCallbackFunc func(string, string)

// MethodGroup: Auth

// NewCommonAPI New CommonAPI
func NewCommonAPI(lr repo.LockedRepo, secret *jwt.HMACSHA, callback dtypes.SessionCallbackFunc) (CommonAPI, error) {
	commAPI := CommonAPI{
		APISecret:       secret,
		SessionCallBack: callback,
	}

	return commAPI, nil
}

func (a *CommonAPI) AuthVerify(ctx context.Context, token string) ([]auth.Permission, error) {
	var payload jwtPayload
	if _, err := jwt.Verify([]byte(token), a.APISecret, &payload); err != nil {
		return nil, xerrors.Errorf("JWT Verification failed: %w", err)
	}

	return payload.Allow, nil
}

func (a *CommonAPI) AuthNew(ctx context.Context, perms []auth.Permission) (string, error) {
	p := jwtPayload{
		Allow: perms, // TODO: consider checking validity
	}

	tk, err := jwt.Sign(&p, a.APISecret)
	if err != nil {
		return "", err
	}

	return string(tk), nil
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
	v, err := api.VersionForType(types.RunningNodeType)
	if err != nil {
		return api.APIVersion{}, err
	}

	return api.APIVersion{
		Version:    build.UserVersion(),
		APIVersion: v,
	}, nil
}

// Discover returns an OpenRPC document describing an RPC API.
func (a *CommonAPI) Discover(ctx context.Context) (types.OpenRPCDocument, error) {
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
		nodeID := handler.GetNodeID(ctx)
		if nodeID != "" && remoteAddr != "" {
			err := a.SessionCallBack(nodeID, remoteAddr)
			if err != nil {
				return session, err
			}
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

	return os.WriteFile(logFilePath, []byte(""), 0o755)
}
