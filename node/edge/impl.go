package edge

import (
	"context"

	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/node/carfile"
	"github.com/linguohua/titan/node/carfile/carfilestore"
	"github.com/linguohua/titan/node/carfile/downloader"
	"github.com/linguohua/titan/node/common"
	datasync "github.com/linguohua/titan/node/sync"
	"github.com/linguohua/titan/node/validate"
	"golang.org/x/time/rate"

	"github.com/ipfs/go-datastore"
	logging "github.com/ipfs/go-log/v2"
	"github.com/linguohua/titan/node/device"
	"github.com/linguohua/titan/node/download"
)

var log = logging.Logger("edge")

func NewLocalEdgeNode(ctx context.Context, device *device.Device, params *EdgeParams) api.Edge {
	rateLimiter := rate.NewLimiter(rate.Limit(device.GetBandwidthUp()), int(device.GetBandwidthUp()))
	validate := validate.NewValidate(params.CarfileStore, device)

	blockDownload := download.NewBlockDownload(rateLimiter, params.Scheduler, params.CarfileStore, device, validate)

	carfileOeration := carfile.NewCarfileOperation(params.DS, params.CarfileStore, params.Scheduler, downloader.NewCandidate(params.CarfileStore), device)

	// datasync.SyncLocalBlockstore(params.DS, params.CarfileStore)

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

type EdgeParams struct {
	DS              datastore.Batching
	Scheduler       api.Scheduler
	CarfileStore    *carfilestore.CarfileStore
	DownloadSrvKey  string
	DownloadSrvAddr string
	IPFSAPI         string
}

func (edge *Edge) WaitQuiet(ctx context.Context) error {
	log.Debug("WaitQuiet")
	return nil
}
