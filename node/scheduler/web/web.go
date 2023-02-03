package web

import (
	"context"
	"time"

	"github.com/linguohua/titan/node/scheduler/db/cache"

	logging "github.com/ipfs/go-log/v2"
	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/node/scheduler/db/persistent"
)

var log = logging.Logger("web")

type web struct {
	scheduler api.Scheduler
}

func NewWeb(scheduler api.Scheduler) api.Web {
	return &web{scheduler: scheduler}
}

func (w *web) ListNodes(ctx context.Context, cursor int, count int) (api.ListNodesRsp, error) {
	rsp := api.ListNodesRsp{Data: make([]api.DevicesInfo, 0)}

	nodes, total, err := persistent.GetDB().GetNodes(cursor, count)
	if err != nil {
		return rsp, err
	}

	deviceInValidator := make(map[string]struct{})
	validatorList, err := cache.GetDB().GetValidatorsWithList()
	if err != nil {
		log.Errorf("get validator list: %v", err)
	}
	for _, id := range validatorList {
		deviceInValidator[id] = struct{}{}
	}

	deviceInfos := make([]api.DevicesInfo, 0)
	for _, node := range nodes {
		deviceInfo, err := cache.GetDB().GetDeviceInfo(node.DeviceID)
		if err != nil {
			log.Errorf("getNodeInfo: %s ,deviceID : %s", err.Error(), node.DeviceID)
			continue
		}
		switch node.IsOnline {
		case false:
			deviceInfo.DeviceStatus = "offline"
		case true:
			deviceInfo.DeviceStatus = "online"
		default:
			deviceInfo.DeviceStatus = "abnormal"
		}

		_, exist := deviceInValidator[node.DeviceID]
		if exist {
			deviceInfo.NodeType = api.NodeValidate
		}

		deviceInfos = append(deviceInfos, *deviceInfo)
	}

	rsp.Data = deviceInfos
	rsp.Total = total

	return rsp, nil
}

func (w *web) GetNodeInfoByID(ctx context.Context, deviceID string) (api.DevicesInfo, error) {
	return w.scheduler.GetDevicesInfo(ctx, deviceID)
}

func (w *web) ListBlockDownloadInfo(ctx context.Context, req api.ListBlockDownloadInfoReq) (api.ListBlockDownloadInfoRsp, error) {
	startTime := time.Unix(req.StartTime, 0)
	endTime := time.Unix(req.EndTime, 0)

	downloadInfos, total, err := persistent.GetDB().GetBlockDownloadInfos(req.DeviceID, startTime, endTime, req.Cursor, req.Count)
	if err != nil {
		return api.ListBlockDownloadInfoRsp{}, nil
	}
	return api.ListBlockDownloadInfoRsp{
		Data:  downloadInfos,
		Total: total,
	}, nil
}

func (w *web) ListNodeConnectionLog(ctx context.Context, req api.ListNodeConnectionLogReq) (api.ListNodeConnectionLogRsp, error) {
	startTime := time.Unix(req.StartTime, 0)
	endTime := time.Unix(req.EndTime, 0)

	logs, total, err := persistent.GetDB().GetNodeConnectionLogs(req.NodeID, startTime, endTime, req.Cursor, req.Count)
	if err != nil {
		return api.ListNodeConnectionLogRsp{}, err
	}

	return api.ListNodeConnectionLogRsp{Total: total, Data: logs}, nil
}

func (w *web) GetCacheTaskInfos(ctx context.Context, req api.ListCacheInfosReq) (api.ListCacheInfosRsp, error) {
	startTime := time.Unix(req.StartTime, 0)
	endTime := time.Unix(req.EndTime, 0)

	info, err := persistent.GetDB().GetCacheTaskInfos(startTime, endTime, req.Cursor, req.Count)
	if err != nil {
		return api.ListCacheInfosRsp{}, err
	}

	return *info, nil
}

// func (w *web) StatCaches(ctx context.Context) (api.StatCachesRsp, error) {
// 	return api.StatCachesRsp{}, nil
// }

// cache manager
func (w *web) AddCacheTask(ctx context.Context, carFileCID string, reliability int, expireTime time.Time) error {
	return w.scheduler.CacheCarfile(ctx, carFileCID, reliability, expireTime)
}

func (w *web) ListCacheTasks(ctx context.Context, cursor int, count int) (api.ListCacheTasksRsp, error) {
	return api.ListCacheTasksRsp{}, nil
}

func (w *web) GetCacheTaskInfo(ctx context.Context, carFileCID string) (api.CarfileRecordInfo, error) {
	return w.scheduler.GetCarfileRecord(ctx, carFileCID)
}

func (w *web) CancelCacheTask(ctx context.Context, carFileCID string) error {
	return nil
}

func (w *web) GetCarfileByCID(ctx context.Context, carFileCID string) (api.WebCarfile, error) {
	return api.WebCarfile{}, nil
}

func (w *web) GetBlocksByCarfileCID(ctx context.Context, carFileCID string) ([]api.WebBlock, error) {
	return []api.WebBlock{}, nil
}

func (w *web) RemoveCarfile(ctx context.Context, carFileCID string) error {
	return w.scheduler.RemoveCarfile(ctx, carFileCID)
}

func (w *web) ListValidateResult(ctx context.Context, cursor int, count int) (api.ListValidateResultRsp, error) {
	results, total, err := persistent.GetDB().GetValidateResults(cursor, count)
	if err != nil {
		return api.ListValidateResultRsp{}, nil
	}

	return api.ListValidateResultRsp{Total: total, Data: results}, nil
}

func (w *web) SetupValidation(ctx context.Context, enable bool) error {
	w.scheduler.ValidateSwitch(ctx, enable)
	return nil
}

func (w *web) GetSystemInfo(ctx context.Context) (api.BaseInfo, error) {
	info, err := cache.GetDB().GetBaseInfo()
	if err != nil {
		return api.BaseInfo{}, err
	}

	return *info, nil
}

func (w *web) GetSummaryValidateMessage(ctx context.Context, startTime, endTime time.Time, pageNumber, pageSize int) (*api.SummeryValidateResult, error) {
	if pageNumber <= 0 {
		pageNumber = 1
	}
	if pageSize > 500 {
		pageSize = 500
	}
	svm, err := persistent.GetDB().SummaryValidateMessage(startTime, endTime, pageNumber, pageSize)
	if err != nil {
		return nil, err
	}

	return svm, nil
}
