package web

import (
	"context"
	"time"

	"github.com/linguohua/titan/node/scheduler/node"

	logging "github.com/ipfs/go-log/v2"
	"github.com/linguohua/titan/api"
)

var log = logging.Logger("web")

type web struct {
	NodeMgr *node.Manager
}

func NewWeb(nodeMgr *node.Manager) api.Web {
	return &web{
		NodeMgr: nodeMgr,
	}
}

func (w *web) ListNodes(ctx context.Context, cursor int, count int) (api.ListNodesRsp, error) {
	rsp := api.ListNodesRsp{Data: make([]api.DeviceInfo, 0)}

	nodes, total, err := w.NodeMgr.NodeMgrDB.ListNodeIDs(cursor, count)
	if err != nil {
		return rsp, err
	}

	deviceInValidator := make(map[string]struct{})
	validatorList, err := w.NodeMgr.NodeMgrDB.GetValidatorsWithList(w.NodeMgr.ServerID)
	if err != nil {
		log.Errorf("get validator list: %v", err)
	}
	for _, id := range validatorList {
		deviceInValidator[id] = struct{}{}
	}

	deviceInfos := make([]api.DeviceInfo, 0)
	for _, nodeID := range nodes {
		deviceInfo, err := w.NodeMgr.NodeMgrDB.LoadNodeInfo(nodeID)
		if err != nil {
			log.Errorf("getNodeInfo: %s ,nodeID : %s", err.Error(), nodeID)
			continue
		}

		// deviceInfo.DeviceStatus = "offline"
		// if node.IsOnline {
		// 	deviceInfo.DeviceStatus = "online"
		// }

		_, exist := deviceInValidator[nodeID]
		if exist {
			deviceInfo.NodeType = api.NodeValidate
		}

		deviceInfos = append(deviceInfos, *deviceInfo)
	}

	rsp.Data = deviceInfos
	rsp.Total = total

	return rsp, nil
}

func (w *web) GetNodeInfoByID(ctx context.Context, nodeID string) (api.DeviceInfo, error) {
	// node datas
	deviceInfo, err := w.NodeMgr.NodeMgrDB.LoadNodeInfo(nodeID)
	if err != nil {
		log.Errorf("getNodeInfo: %s ,nodeID : %s", err.Error(), nodeID)
		return api.DeviceInfo{}, err
	}

	isOnline := w.NodeMgr.GetCandidateNode(nodeID) != nil
	if !isOnline {
		isOnline = w.NodeMgr.GetEdgeNode(nodeID) != nil
	}

	deviceInfo.DeviceStatus = getDeviceStatus(isOnline)

	return *deviceInfo, nil
}

const (
	// StatusOffline node offline
	StatusOffline = "offline"
	// StatusOnline node online
	StatusOnline = "online"
)

func getDeviceStatus(isOnline bool) string {
	switch isOnline {
	case true:
		return StatusOnline
	default:
		return StatusOffline
	}
}

func (w *web) ListBlockDownloadInfo(ctx context.Context, req api.ListBlockDownloadInfoReq) (api.ListBlockDownloadInfoRsp, error) {
	startTime := time.Unix(req.StartTime, 0)
	endTime := time.Unix(req.EndTime, 0)

	downloadInfos, total, err := w.NodeMgr.CarfileDB.GetBlockDownloadInfos(req.NodeID, startTime, endTime, req.Cursor, req.Count)
	if err != nil {
		return api.ListBlockDownloadInfoRsp{}, nil
	}
	return api.ListBlockDownloadInfoRsp{
		Data:  downloadInfos,
		Total: total,
	}, nil
}

func (w *web) GetReplicaInfos(ctx context.Context, req api.ListCacheInfosReq) (api.ListCacheInfosRsp, error) {
	startTime := time.Unix(req.StartTime, 0)
	endTime := time.Unix(req.EndTime, 0)

	info, err := w.NodeMgr.CarfileDB.GetReplicaInfos(startTime, endTime, req.Cursor, req.Count)
	if err != nil {
		return api.ListCacheInfosRsp{}, err
	}

	return *info, nil
}

func (w *web) GetBlocksByCarfileCID(ctx context.Context, carFileCID string) ([]api.WebBlock, error) {
	return []api.WebBlock{}, nil
}

// func (w *web) ListValidateResult(ctx context.Context, cursor int, count int) (api.ListValidateResultRsp, error) {
// 	results, total, err := GetValidateResults(cursor, count)
// 	if err != nil {
// 		return api.ListValidateResultRsp{}, nil
// 	}

// 	return api.ListValidateResultRsp{Total: total, Data: results}, nil
// }

func (w *web) GetSystemInfo(ctx context.Context) (api.SystemBaseInfo, error) {
	// TODO get info from db

	return api.SystemBaseInfo{}, nil
}

func (w *web) GetSummaryValidateMessage(ctx context.Context, startTime, endTime time.Time, pageNumber, pageSize int) (*api.SummeryValidateResult, error) {
	svm, err := w.NodeMgr.NodeMgrDB.ValidateResultInfos(startTime, endTime, pageNumber, pageSize)
	if err != nil {
		return nil, err
	}

	return svm, nil
}
