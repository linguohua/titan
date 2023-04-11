package modules

import (
	"context"

	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/node/asset/fetcher"
	"github.com/linguohua/titan/node/candidate"
	"github.com/linguohua/titan/node/config"
	"go.uber.org/fx"
)

func NewBlockFetcherFromIPFS(cfg *config.CandidateCfg) fetcher.BlockFetcher {
	log.Info("ipfs-api " + cfg.IpfsAPIURL)
	return fetcher.NewIPFSClient(cfg.IpfsAPIURL, cfg.FetchBlockTimeout, cfg.FetchBlockRetry)
}

// NewTCPServer returns a new TCP server instance.
func NewTCPServer(lc fx.Lifecycle, cfg *config.CandidateCfg, schedulerAPI api.Scheduler) *candidate.TCPServer {
	srv := candidate.NewTCPServer(cfg, schedulerAPI)

	lc.Append(fx.Hook{
		OnStart: func(context.Context) error {
			go srv.StartTCPServer()
			return nil
		},
		OnStop: srv.Stop,
	})

	return srv
}
