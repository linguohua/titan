package web

import (
	"context"

	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/node/scheduler/db/persistent"
)

type web struct {
	scheduler api.Scheduler
}

func NewWeb(scheduler api.Scheduler) api.Web {
	return &web{scheduler: scheduler}
}

func (w *web) ListNodes(ctx context.Context, cursor int, count int) ([]api.WebNode, error) {
	webNodes := make([]api.WebNode, 0)
	nodes, err := persistent.GetDB().GetNodes(cursor, count)
	if err != nil {
		return webNodes, nil
	}

	for _, node := range nodes {
		webNode := api.WebNode{NodeID: node.DeviceID, NodeName: node.DeviceID}
		if len(webNode.NodeName) > 10 {
			webNode.NodeName = webNode.NodeName[0:10]
		}

		webNodes = append(webNodes, webNode)
	}

	return webNodes, nil
}

func (w *web) GetNodeInfoByID(ctx context.Context, deviceID string) (api.DevicesInfo, error) {
	return w.scheduler.GetDevicesInfo(ctx, deviceID)
}

func (w *web) ListBlockDownloadInfo(ctx context.Context, req api.ListBlockDownloadInfoReq) ([]api.BlockDownloadInfo, error) {
	return persistent.GetDB().GetBlockDownloadInfos(req.DeviceID, req.StartTime, req.EndTime, req.Cursor, req.Count)
}

func (w *web) ListCaches(ctx context.Context, req api.ListCachesReq) ([]api.WebCarfile, error) {
	return []api.WebCarfile{}, nil
}

func (w *web) StatCaches(ctx context.Context) (api.StatCachesRsp, error) {
	return api.StatCachesRsp{}, nil
}

func (w *web) ListNodeConnectionLog(ctx context.Context, cursor int, count int) ([]api.NodeConnectionLog, error) {
	return []api.NodeConnectionLog{}, nil
}

// cache manager
func (w *web) AddCacheTask(ctx context.Context, carFileCID string, reliability int) error {
	return nil
}
func (w *web) ListCacheTask(ctx context.Context, cursor int, count int) ([]api.CacheDataInfo, error) {
	return []api.CacheDataInfo{}, nil
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

func (w *web) ListValidators(ctx context.Context, cursor int, count int) (api.ListValidatorsRsp, error) {
	return api.ListValidatorsRsp{}, nil
}
func (w *web) ListVadiateResult(ctx context.Context, cursor int, count int) ([]api.WebValidateResult, error) {
	return []api.WebValidateResult{}, nil
}
func (w *web) SetupValidation(ctx context.Context, DeviceID string) error {
	return nil
}