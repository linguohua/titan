package scheduler

import (
	"context"

	logging "github.com/ipfs/go-log/v2"

	"titan-ultra-network/api"
	"titan-ultra-network/api/client"
	"titan-ultra-network/journal/alerting"

	"github.com/filecoin-project/go-jsonrpc/auth"
	"github.com/google/uuid"
)

// NewLocalScheduleNode NewLocalScheduleNode
func NewLocalScheduleNode() api.Scheduler {
	return Scheduler{}
}

var log = logging.Logger("scheduler")

// Scheduler 定义类型
type Scheduler struct{}

// EdgeNodeConnect edge connect
func (s Scheduler) EdgeNodeConnect(ctx context.Context, url string) error {
	// Connect to scheduler
	log.Infof("EdgeNodeConnect edge url : %v ", url)
	edgeAPI, closer, err := client.NewEdge(ctx, url, nil)
	if err != nil {
		log.Errorf("edgeAPI NewEdge err : %v", err)
		return err
	}

	// 拉取设备数据
	deviceID, err := edgeAPI.DeviceID(ctx)
	if err != nil {
		log.Errorf("edgeAPI DeviceID err : %v", err)
		return err
	}

	log.Infof("edgeAPI Version deviceID : %v", deviceID)

	edgeNode := EdgeNode{
		addr:     url,
		edgeAPI:  edgeAPI,
		closer:   closer,
		deviceID: deviceID,
		userID:   url,
	}
	addEdgeNode(&edgeNode)

	return nil
}

// AuthNew Auth
func (s Scheduler) AuthNew(ctx context.Context, perms []auth.Permission) ([]byte, error) {
	return nil, nil
}

// AuthVerify Verify
func (s Scheduler) AuthVerify(ctx context.Context, token string) ([]auth.Permission, error) {
	return nil, nil
}

// LogList Log List
func (s Scheduler) LogList(context.Context) ([]string, error) {
	return nil, nil
}

// LogSetLevel Set Level
func (s Scheduler) LogSetLevel(context.Context, string, string) error {
	return nil
}

// LogAlerts Log Alerts
func (s Scheduler) LogAlerts(ctx context.Context) ([]alerting.Alert, error) {
	return nil, nil
}

// Version Version
func (s Scheduler) Version(context.Context) (api.APIVersion, error) {
	v, err := api.VersionForType(api.NodeScheduler)
	if err != nil {
		return api.APIVersion{}, err
	}

	return api.APIVersion{Version: "1.52", APIVersion: v}, nil
}

// Discover Discover
func (s Scheduler) Discover(ctx context.Context) (api.OpenRPCDocument, error) {
	return nil, nil
}

// Shutdown Shutdown
func (s Scheduler) Shutdown(context.Context) error {
	return nil
}

// Session Session
func (s Scheduler) Session(context.Context) (uuid.UUID, error) {
	return uuid.UUID{}, nil
}

// Closing Closing
func (s Scheduler) Closing(context.Context) (<-chan struct{}, error) {
	return nil, nil
}
