package edge

import (
	"context"

	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/node/carfile"
	"github.com/linguohua/titan/node/common"
	"github.com/linguohua/titan/node/helper"
	datasync "github.com/linguohua/titan/node/sync"
	"github.com/linguohua/titan/node/validate"
	"golang.org/x/time/rate"

	logging "github.com/ipfs/go-log/v2"
	"github.com/linguohua/titan/node/device"
	"github.com/linguohua/titan/node/download"
)

var log = logging.Logger("edge")

func NewLocalEdgeNode(ctx context.Context, device *device.Device, params *helper.NodeParams) api.Edge {
	rateLimiter := rate.NewLimiter(rate.Limit(device.GetBandwidthUp()), int(device.GetBandwidthUp()))
	validate := validate.NewValidate(params.CarfileStore, device)

	blockDownload := download.NewBlockDownload(rateLimiter, params, device, validate)

	carfileOeration := carfile.NewCarfileOperation(params.CarfileStore, params.Scheduler, carfile.NewCandidate(params.CarfileStore), device)

	datasync.SyncLocalBlockstore(params.DS, params.CarfileStore)

	edge := &Edge{
		Device:           device,
		CarfileOperation: carfileOeration,
		BlockDownload:    blockDownload,
		Validate:         validate,
		DataSync:         datasync.NewDataSync(params.DS),
	}

	return edge
}

type Edge struct {
	*common.CommonAPI
	*device.Device
	*carfile.CarfileOperation
	*download.BlockDownload
	*validate.Validate
	*datasync.DataSync
}

func (edge *Edge) WaitQuiet(ctx context.Context) error {
	log.Debug("WaitQuiet")
	return nil
}
