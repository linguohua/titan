package scheduler

import (
	"context"

	logging "github.com/ipfs/go-log/v2"

	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/api/client"
	"github.com/linguohua/titan/node/common"
	"github.com/linguohua/titan/node/scheduler/db"
)

var (
	log     = logging.Logger("scheduler")
	cacheDB db.CacheDB
)

// NewLocalScheduleNode NewLocalScheduleNode
func NewLocalScheduleNode(c db.CacheDB) api.Scheduler {
	cacheDB = c
	return Scheduler{}
}

// Scheduler 定义类型
type Scheduler struct {
	common.CommonAPI
}

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

// CacheData Cache Data
func (s Scheduler) CacheData(ctx context.Context, cids, deviceIDs []string) error {
	return CacheData(cids, deviceIDs)
}

// LoadData Load Data
func (s Scheduler) LoadData(ctx context.Context, cid, deviceID string) ([]byte, error) {
	return LoadData(cid, deviceID)
}

// CandidateNodeConnect Candidate connect
func (s Scheduler) CandidateNodeConnect(ctx context.Context, url string) error {
	return nil
}
