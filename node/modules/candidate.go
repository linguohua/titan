package modules

import (
	"context"

	"github.com/linguohua/titan/node/candidate"
	"github.com/linguohua/titan/node/carfile/carfilestore"
	"github.com/linguohua/titan/node/carfile/downloader"
	"github.com/linguohua/titan/node/config"
	"github.com/linguohua/titan/node/modules/dtypes"
	"go.uber.org/fx"
)

type DeviceParams struct {
	fx.In

	NodeID        dtypes.NodeID
	InternalIP    dtypes.InternalIP
	CarfileStore  *carfilestore.CarfileStore
	BandwidthUP   int64
	BandwidthDown int64
}

func NewDownloadBlockerFromIPFS(cfg *config.CandidateCfg, carfileStore *carfilestore.CarfileStore) downloader.DownloadBlockser {
	log.Info("ipfs-api " + cfg.IpfsApiURL)
	return downloader.NewIPFS(cfg.IpfsApiURL, carfileStore)
}

func NewTcpServer(lc fx.Lifecycle, cfg *config.CandidateCfg, blockWait *candidate.BlockWaiter) *candidate.TCPServer {
	srv := candidate.NewTCPServer(cfg, blockWait)

	lc.Append(fx.Hook{
		OnStart: func(context.Context) error {
			go srv.StartTcpServer()
			return nil
		},
		OnStop: srv.Stop,
	})

	return srv
}
