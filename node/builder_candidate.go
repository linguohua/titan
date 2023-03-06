package node

import (
	"errors"
	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/node/candidate"
	"github.com/linguohua/titan/node/carfile"
	"github.com/linguohua/titan/node/carfile/carfilestore"
	"github.com/linguohua/titan/node/carfile/downloader"
	"github.com/linguohua/titan/node/config"
	"github.com/linguohua/titan/node/device"
	"github.com/linguohua/titan/node/download"
	"github.com/linguohua/titan/node/modules"
	"github.com/linguohua/titan/node/modules/dtypes"
	"github.com/linguohua/titan/node/repo"
	datasync "github.com/linguohua/titan/node/sync"
	"github.com/linguohua/titan/node/validate"
	"go.uber.org/fx"
	"golang.org/x/time/rate"
	"golang.org/x/xerrors"
)

func Candidate(out *api.Candidate) Option {
	return Options(
		ApplyIf(func(s *Settings) bool { return s.Config },
			Error(errors.New("the Candidate option must be set before Config option")),
		),

		func(s *Settings) error {
			s.nodeType = repo.Candidate
			return nil
		},

		func(s *Settings) error {
			resAPI := &candidate.Candidate{}
			s.invokes[ExtractApiKey] = fx.Populate(resAPI)
			*out = resAPI
			return nil
		},
	)
}

func ConfigCandidate(c interface{}) Option {
	cfg, ok := c.(*config.CandidateCfg)
	if !ok {
		return Error(xerrors.Errorf("invalid config from repo, got: %T", c))
	}
	log.Info("start to config candidate")

	return Options(
		Override(new(*config.CandidateCfg), cfg),
		Override(new(*device.Device), modules.NewDevice(cfg.BandwidthUp, cfg.BandwidthDown)),
		Override(new(dtypes.CarfileStoreType), dtypes.CarfileStoreType(cfg.CarfileStoreType)),
		Override(new(dtypes.CarfileStorePath), dtypes.CarfileStorePath(cfg.CarfileStorePath)),
		Override(new(*carfilestore.CarfileStore), modules.NewCarfileStore),
		Override(new(*validate.Validate), validate.NewValidate),
		Override(new(*rate.Limiter), modules.NewRateLimiter),
		Override(new(*download.BlockDownload), download.NewBlockDownload),
		Override(new(*carfile.CarfileOperation), carfile.NewCarfileOperation),
		Override(new(downloader.DownloadBlockser), modules.NewIPFSDownloadBlocker),
		Override(new(*datasync.DataSync), datasync.NewDataSync),
		Override(new(*candidate.BlockWaiter), candidate.NewBlockWaiter),
		Override(new(*candidate.TCPServer), candidate.NewTCPServer),
	)
}
