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

	nodes, total, err := w.NodeMgr.NodeMgrDB.ListDeviceIDs(cursor, count)
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
	for _, deviceID := range nodes {
		deviceInfo, err := w.NodeMgr.NodeMgrDB.LoadNodeInfo(deviceID)
		if err != nil {
			log.Errorf("getNodeInfo: %s ,deviceID : %s", err.Error(), deviceID)
			continue
		}

		// deviceInfo.DeviceStatus = "offline"
		// if node.IsOnline {
		// 	deviceInfo.DeviceStatus = "online"
		// }

		_, exist := deviceInValidator[deviceID]
		if exist {
			deviceInfo.NodeType = api.NodeValidate
		}

		deviceInfos = append(deviceInfos, *deviceInfo)
	}

	rsp.Data = deviceInfos
	rsp.Total = total

	return rsp, nil
}

func (w *web) GetNodeInfoByID(ctx context.Context, deviceID string) (api.DeviceInfo, error) {
	// node datas
	deviceInfo, err := w.NodeMgr.NodeMgrDB.LoadNodeInfo(deviceID)
	if err != nil {
		log.Errorf("getNodeInfo: %s ,deviceID : %s", err.Error(), deviceID)
		return api.DeviceInfo{}, err
	}

	isOnline := w.NodeMgr.GetCandidateNode(deviceID) != nil
	if !isOnline {
		isOnline = w.NodeMgr.GetEdgeNode(deviceID) != nil
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

	downloadInfos, total, err := w.NodeMgr.CarfileDB.GetBlockDownloadInfos(req.DeviceID, startTime, endTime, req.Cursor, req.Count)
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
