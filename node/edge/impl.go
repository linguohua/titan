package edge

import (
	"context"

	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/build"
	"github.com/linguohua/titan/lib/p2p"
	"github.com/linguohua/titan/node/common"
	"github.com/linguohua/titan/node/validate"
	"github.com/linguohua/titan/stores"
	"golang.org/x/time/rate"

	"github.com/ipfs/go-datastore"
	logging "github.com/ipfs/go-log/v2"
	"github.com/linguohua/titan/node/block"
	"github.com/linguohua/titan/node/device"
	"github.com/linguohua/titan/node/download"
)

var log = logging.Logger("edge")

func NewLocalEdgeNode(ctx context.Context, params *EdgeParams) api.Edge {
	addrs, err := build.BuiltinBootstrap()
	if err != nil {
		log.Fatal(err)
	}

	exchange, err := p2p.Bootstrap(ctx, addrs)
	if err != nil {
		log.Fatal(err)
	}

	rateLimiter := rate.NewLimiter(rate.Limit(params.Device.BandwidthUp), int(params.Device.BandwidthUp))
	blockDownload := download.NewBlockDownload(rateLimiter, params.BlockStore, params.DownloadSrvKey, params.DownloadSrvAddr, params.Device.InternalIP)
	params.Device.SetBlockDownload(blockDownload)

	block := block.NewBlock(params.DS, params.BlockStore, params.Scheduler, &block.Candidate{}, exchange, params.Device.DeviceID)

	validate := validate.NewValidate(blockDownload, block, params.Device.DeviceID)

	edge := &Edge{
		Device:        params.Device,
		Block:         block,
		BlockDownload: blockDownload,
		Validate:      validate,
	}

	return edge
}

type EdgeParams struct {
	DS              datastore.Batching
	Scheduler       api.Scheduler
	BlockStore      stores.BlockStore
	Device          *device.Device
	DownloadSrvKey  string
	DownloadSrvAddr string
}

type Edge struct {
	*common.CommonAPI
	*device.Device
	*block.Block
	*download.BlockDownload
	*validate.Validate
}

func (edge *Edge) WaitQuiet(ctx context.Context) error {
	log.Debug("WaitQuiet")
	return nil
}
