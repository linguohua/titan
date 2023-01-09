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
	"github.com/linguohua/titan/node/block"
	"github.com/linguohua/titan/node/device"
	"github.com/linguohua/titan/node/download"
)

var log = logging.Logger("edge")

func NewLocalEdgeNode(ctx context.Context, device *device.Device, params *helper.NodeParams) api.Edge {
	rateLimiter := rate.NewLimiter(rate.Limit(device.GetBandwidthUp()), int(device.GetBandwidthUp()))

	block := block.NewBlock(params.DS, params.BlockStore, params.Scheduler, &block.Candidate{}, device, params.IPFSAPI)

	validate := validate.NewValidate(block, device)
	blockDownload := download.NewBlockDownload(rateLimiter, params, device, validate)

	carfileOeration := carfile.NewCarfileOperation(params.DS, params.BlockStore, params.Scheduler, &carfile.Candidate{}, device)

	datasync.SyncLocalBlockstore(params.DS, params.BlockStore, block)

	edge := &Edge{
		Device:           device,
		Block:            block,
		CarfileOperation: carfileOeration,
		BlockDownload:    blockDownload,
		Validate:         validate,
		DataSync:         datasync.NewDataSync(block, params.DS),
	}

	return edge
}

type Edge struct {
	*common.CommonAPI
	*device.Device
	*block.Block
	*carfile.CarfileOperation
	*download.BlockDownload
	*validate.Validate
	*datasync.DataSync
}

func (edge *Edge) WaitQuiet(ctx context.Context) error {
	log.Debug("WaitQuiet")
	return nil
}
