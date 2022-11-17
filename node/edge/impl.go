package edge

import (
	"context"

	"github.com/linguohua/titan/api"
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
	// addrs, err := build.BuiltinBootstrap()
	// if err != nil {
	// 	log.Fatal(err)
	// }

	// exchange, err := p2p.Bootstrap(ctx, addrs)
	// if err != nil {
	// 	log.Fatal(err)
	// }

	rateLimiter := rate.NewLimiter(rate.Limit(device.GetBandwidthUp()), int(device.GetBandwidthUp()))
	blockDownload := download.NewBlockDownload(rateLimiter, params, device)

	block := block.NewBlock(params.DS, params.BlockStore, params.Scheduler, &block.Candidate{}, params.IPFSGateway, device.GetDeviceID())

	validate := validate.NewValidate(blockDownload, block, device.GetDeviceID())

	edge := &Edge{
		Device:        device,
		Block:         block,
		BlockDownload: blockDownload,
		Validate:      validate,
		DataSync:      datasync.NewDataSync(block),
	}

	return edge
}

type Edge struct {
	*common.CommonAPI
	*device.Device
	*block.Block
	*download.BlockDownload
	*validate.Validate
	*datasync.DataSync
}

func (edge *Edge) WaitQuiet(ctx context.Context) error {
	log.Debug("WaitQuiet")
	return nil
}
