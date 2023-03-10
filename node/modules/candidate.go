package modules

import (
	"context"

	"github.com/linguohua/titan/node/candidate"
	"github.com/linguohua/titan/node/carfile/fetcher"
	"github.com/linguohua/titan/node/carfile/store"
	"github.com/linguohua/titan/node/config"
	"github.com/linguohua/titan/node/modules/dtypes"
	"go.uber.org/fx"
)

type NodeParams struct {
	fx.In

	NodeID        dtypes.NodeID
	InternalIP    dtypes.InternalIP
	CarfileStore  *store.CarfileStore
	BandwidthUP   int64
	BandwidthDown int64
}

func NewBlockFetcherFromIPFS(cfg *config.CandidateCfg) fetcher.BlockFetcher {
	log.Info("ipfs-api " + cfg.IpfsApiURL)
	return fetcher.NewIPFS(cfg.IpfsApiURL, cfg.FetchBlockTimeout, cfg.FetchBlockFailedRetry)
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
