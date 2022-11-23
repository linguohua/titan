package web

import (
	"context"

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

	deviceInfos := make([]api.DevicesInfo, 0)
	for _, node := range nodes {
		deviceInfo, err := w.scheduler.GetDevicesInfo(context.Background(), node.DeviceID)
		if err != nil {
			log.Errorf("ListNodes, get device info error:%s", err.Error())
			continue
		}

		deviceInfos = append(deviceInfos, deviceInfo)
	}

	rsp.Data = deviceInfos
	rsp.Total = total

	return rsp, nil
}

func (w *web) GetNodeInfoByID(ctx context.Context, deviceID string) (api.DevicesInfo, error) {
	return w.scheduler.GetDevicesInfo(ctx, deviceID)
}

func (w *web) ListBlockDownloadInfo(ctx context.Context, req api.ListBlockDownloadInfoReq) (api.ListBlockDownloadInfoRsp, error) {
	downloadInfos, total, err := persistent.GetDB().GetBlockDownloadInfos(req.DeviceID, req.StartTime, req.EndTime, req.Cursor, req.Count)
	if err != nil {
		return api.ListBlockDownloadInfoRsp{}, nil
	}
	return api.ListBlockDownloadInfoRsp{
		Data:  downloadInfos,
		Total: total,
	}, nil
}

func (w *web) ListCaches(ctx context.Context, req api.ListCachesReq) (api.ListCachesRsp, error) {
	return api.ListCachesRsp{}, nil
}

func (w *web) StatCaches(ctx context.Context) (api.StatCachesRsp, error) {
	return api.StatCachesRsp{}, nil
}

func (w *web) ListNodeConnectionLog(ctx context.Context, req api.ListNodeConnectionLogReq) (api.ListNodeConnectionLogRsp, error) {
	return api.ListNodeConnectionLogRsp{}, nil
}

// cache manager
func (w *web) AddCacheTask(ctx context.Context, carFileCID string, reliability int) error {
	return nil
}
func (w *web) ListCacheTasks(ctx context.Context, cursor int, count int) (api.ListCacheTasksRsp, error) {
	return api.ListCacheTasksRsp{}, nil
}
func (w *web) GetCacheTaskInfo(ctx context.Context, carFileCID string) (api.CacheDataInfo, error) {
	return api.CacheDataInfo{}, nil
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
	return nil
}

func (w *web) GetValidationInfo(ctx context.Context) (api.ValidationInfo, error) {
	return api.ValidationInfo{}, nil
}
func (w *web) ListValidateResult(ctx context.Context, cursor int, count int) (api.ListValidateResultRsp, error) {
	return api.ListValidateResultRsp{}, nil
}
func (w *web) SetupValidation(ctx context.Context, DeviceID string) error {
	return nil
}
