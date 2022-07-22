package scheduler

import (
	"context"

	"titan-ultra-network/api"
	"titan-ultra-network/journal/alerting"

	"github.com/filecoin-project/go-jsonrpc/auth"
	"github.com/google/uuid"
)

// NewLocalScheduleNode NewLocalScheduleNode
func NewLocalScheduleNode() api.Scheduler {
	var student Scheduler

	var person api.Scheduler
	person = student

	return person
}

// Scheduler 定义类型
type Scheduler struct{}

func (s Scheduler) EdgeNodeConnect(ctx context.Context, str string) error {
	return nil
}

func (s Scheduler) AuthNew(ctx context.Context, perms []auth.Permission) ([]byte, error) {
	return nil, nil
}

func (s Scheduler) AuthVerify(ctx context.Context, token string) ([]auth.Permission, error) {
	return nil, nil
}

func (s Scheduler) LogList(context.Context) ([]string, error) {
	return nil, nil
}

func (s Scheduler) LogSetLevel(context.Context, string, string) error {
	return nil
}

func (s Scheduler) LogAlerts(ctx context.Context) ([]alerting.Alert, error) {
	return nil, nil
}

func (s Scheduler) Version(context.Context) (api.APIVersion, error) {
	return api.APIVersion{Version: "1.52"}, nil
}

func (s Scheduler) Discover(ctx context.Context) (api.OpenRPCDocument, error) {
	return nil, nil
}

func (s Scheduler) Shutdown(context.Context) error {
	return nil
}

func (s Scheduler) Session(context.Context) (uuid.UUID, error) {
	return uuid.UUID{}, nil
}

func (s Scheduler) Closing(context.Context) (<-chan struct{}, error) {
	return nil, nil
}
