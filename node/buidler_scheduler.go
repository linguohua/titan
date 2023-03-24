package node

import (
	"crypto/rand"
	"crypto/rsa"
	"errors"

	"github.com/jmoiron/sqlx"
	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/node/config"
	"github.com/linguohua/titan/node/modules"
	"github.com/linguohua/titan/node/modules/dtypes"
	"github.com/linguohua/titan/node/repo"
	"github.com/linguohua/titan/node/scheduler"
	"github.com/linguohua/titan/node/scheduler/caching"
	"github.com/linguohua/titan/node/scheduler/db"
	"github.com/linguohua/titan/node/scheduler/election"
	"github.com/linguohua/titan/node/scheduler/node"
	"github.com/linguohua/titan/node/scheduler/sync"
	"github.com/linguohua/titan/node/scheduler/validation"

	"go.uber.org/fx"
	"golang.org/x/xerrors"
)

func Scheduler(out *api.Scheduler) Option {
	return Options(
		ApplyIf(func(s *Settings) bool { return s.Config },
			Error(errors.New("the Scheduler option must be set before Config option")),
		),

		func(s *Settings) error {
			s.nodeType = repo.Scheduler
			return nil
		},

		func(s *Settings) error {
			resAPI := &scheduler.Scheduler{}
			s.invokes[ExtractAPIKey] = fx.Populate(resAPI)
			*out = resAPI
			return nil
		},
	)
}

func ConfigScheduler(c interface{}) Option {
	cfg, ok := c.(*config.SchedulerCfg)
	if !ok {
		return Error(xerrors.Errorf("invalid config from repo, got: %T", c))
	}
	log.Info("start to config scheduler")

	return Options(
		Override(new(dtypes.ServerID), modules.NewServerID),
		Override(new(*config.SchedulerCfg), cfg),
		Override(RegisterEtcd, modules.RegisterToEtcd),
		Override(new(*sqlx.DB), modules.NewDB),
		Override(new(*db.SQLDB), db.NewSQLDB),
		Override(new(*node.Manager), node.NewManager),
		Override(new(dtypes.SessionCallbackFunc), node.KeepaliveCallBackFunc),
		Override(new(dtypes.MetadataDS), modules.Datastore),
		Override(new(*caching.Manager), modules.NewStorageManager),
		Override(new(*sync.DataSync), sync.NewDataSync),
		Override(new(*validation.Validation), modules.NewValidation),
		Override(new(*election.Election), election.NewElection),
		Override(new(*scheduler.EdgeUpdater), scheduler.NewEdgeUpdater),
		Override(new(dtypes.DatabaseAddress), func() dtypes.DatabaseAddress {
			return dtypes.DatabaseAddress(cfg.DatabaseAddress)
		}),
		Override(new(dtypes.SetSchedulerConfigFunc), modules.NewSetSchedulerConfigFunc),
		Override(new(dtypes.GetSchedulerConfigFunc), modules.NewGetSchedulerConfigFunc),
		Override(new(*rsa.PrivateKey), func() (*rsa.PrivateKey, error) {
			return rsa.GenerateKey(rand.Reader, 1024)
		}),
	)
}
