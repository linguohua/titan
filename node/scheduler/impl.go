package scheduler

import (
	"context"

	logging "github.com/ipfs/go-log/v2"

	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/api/client"
	"github.com/linguohua/titan/node/common"
)

var log = logging.Logger("scheduler")

// NewLocalScheduleNode NewLocalScheduleNode
func NewLocalScheduleNode() api.Scheduler {
	return Scheduler{}
}

// Scheduler node
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
	deviceInfo, err := edgeAPI.DeviceInfo(ctx)
	if err != nil {
		log.Errorf("edgeAPI DeviceID err : %v", err)
		return err
	}

	log.Infof("edgeAPI Version deviceID : %v", deviceInfo.DeviceID)

	edgeNode := EdgeNode{
		addr:     url,
		edgeAPI:  edgeAPI,
		closer:   closer,
		deviceID: deviceInfo.DeviceID,
		userID:   url,
		ip:       "192.168.1.1", //"120.24.37.249", // TODO
	}
	addEdgeNode(&edgeNode)

	return nil
}

// CacheResult Cache Data Result
func (s Scheduler) CacheResult(ctx context.Context, deviceID string, cid string, isOK bool) error {
	return nodeCacheResult(deviceID, cid, isOK)
}

// CacheData Cache Data
func (s Scheduler) CacheData(ctx context.Context, cid, deviceID string) error {
	return CacheData(cid, deviceID)
}

// FindNodeWithData find node
func (s Scheduler) FindNodeWithData(ctx context.Context, cid, ip string) (string, error) {
	return GetNodeWithData(cid, ip)
}

// CandidateNodeConnect Candidate connect
func (s Scheduler) CandidateNodeConnect(ctx context.Context, url string) error {
	// candicateAPI, closer, err := client.NewCandicate(ctx, url, nil)
	// if err != nil {
	// 	log.Errorf("edgeAPI NewEdge err : %v", err)
	// 	return err
	// }

	// load device info
	// deviceID, err := candicateAPI.DeviceID(ctx)
	// if err != nil {
	// 	log.Errorf("edgeAPI DeviceID err : %v", err)
	// 	return err
	// }

	// log.Infof("edgeAPI Version deviceID : %v", deviceID)

	// edgeNode := EdgeNode{
	// 	addr:     url,
	// 	edgeAPI:  candicateAPI,
	// 	closer:   closer,
	// 	deviceID: deviceID,
	// 	userID:   url,
	// }
	// addEdgeNode(&edgeNode)
	return nil
}
