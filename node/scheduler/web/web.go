package web

import (
	"context"
	"time"

	"github.com/linguohua/titan/node/scheduler/db/cache"
	"github.com/linguohua/titan/node/scheduler/db/persistent"

	logging "github.com/ipfs/go-log/v2"
	"github.com/linguohua/titan/api"
)

var log = logging.Logger("web")

type web struct {
	scheduler api.Scheduler
}

func NewWeb(scheduler api.Scheduler) api.Web {
	return &web{scheduler: scheduler}
}

func (w *web) ListNodes(ctx context.Context, cursor int, count int) (api.ListNodesRsp, error) {
	rsp := api.ListNodesRsp{Data: make([]api.DeviceInfo, 0)}

	nodes, total, err := persistent.GetDeviceIDs(cursor, count)
	if err != nil {
		return rsp, err
	}

	deviceInValidator := make(map[string]struct{})
	validatorList, err := cache.GetValidatorsWithList()
	if err != nil {
		log.Errorf("get validator list: %v", err)
	}
	for _, id := range validatorList {
		deviceInValidator[id] = struct{}{}
	}

	deviceInfos := make([]api.DeviceInfo, 0)
	for _, deviceID := range nodes {
		deviceInfo, err := persistent.GetDeviceInfo(deviceID)
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
	return w.scheduler.GetDevicesInfo(ctx, deviceID)
}

func (w *web) ListBlockDownloadInfo(ctx context.Context, req api.ListBlockDownloadInfoReq) (api.ListBlockDownloadInfoRsp, error) {
	startTime := time.Unix(req.StartTime, 0)
	endTime := time.Unix(req.EndTime, 0)

	downloadInfos, total, err := persistent.GetBlockDownloadInfos(req.DeviceID, startTime, endTime, req.Cursor, req.Count)
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

	info, err := persistent.GetReplicaInfos(startTime, endTime, req.Cursor, req.Count)
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
	svm, err := persistent.ValidateResultInfos(startTime, endTime, pageNumber, pageSize)
	if err != nil {
		return nil, err
	}

	return svm, nil
}
