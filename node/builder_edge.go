package node

import (
	"errors"

	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/node/carfile"
	"github.com/linguohua/titan/node/carfile/cache"
	"github.com/linguohua/titan/node/carfile/fetcher"
	"github.com/linguohua/titan/node/carfile/storage"
	"github.com/linguohua/titan/node/config"
	"github.com/linguohua/titan/node/device"
	"github.com/linguohua/titan/node/edge"
	"github.com/linguohua/titan/node/modules"
	"github.com/linguohua/titan/node/modules/dtypes"
	"github.com/linguohua/titan/node/repo"
	datasync "github.com/linguohua/titan/node/sync"
	"github.com/linguohua/titan/node/validate"
	"go.uber.org/fx"
	"golang.org/x/time/rate"
	"golang.org/x/xerrors"
)

func Edge(out *api.Edge) Option {
	return Options(
		ApplyIf(func(s *Settings) bool { return s.Config },
			Error(errors.New("the Edge option must be set before Config option")),
		),

		func(s *Settings) error {
			s.nodeType = repo.Edge
			return nil
		},

		func(s *Settings) error {
			resAPI := &edge.Edge{}
			s.invokes[ExtractAPIKey] = fx.Populate(resAPI)
			*out = resAPI
			return nil
		},
	)
}

func ConfigEdge(c interface{}) Option {
	cfg, ok := c.(*config.EdgeCfg)
	if !ok {
		return Error(xerrors.Errorf("invalid config from repo, got: %T", c))
	}
	log.Info("start to config edge")

	return Options(
		Override(new(*config.EdgeCfg), cfg),
		Override(new(*device.Device), modules.NewDevice(cfg.BandwidthUp, cfg.BandwidthDown)),
		Override(new(dtypes.CarfileStorePath), dtypes.CarfileStorePath(cfg.CarfileStorePath)),
		Override(new(*storage.Manager), modules.NewNodeStorageManager),
		Override(new(*cache.Manager), modules.NewCacheManager),
		Override(new(*validate.Validate), validate.NewValidate),
		Override(new(*rate.Limiter), modules.NewRateLimiter),
		Override(new(*carfile.CarfileImpl), carfile.NewCarfileImpl),
		Override(new(*datasync.DataSync), modules.NewDataSync),
		Override(new(fetcher.BlockFetcher), modules.NewBlockFetcherFromCandidate),
	)
}
