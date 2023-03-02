package node

import (
	"errors"
	"github.com/go-redis/redis/v8"
	"github.com/jmoiron/sqlx"
	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/node/common"
	"github.com/linguohua/titan/node/config"
	"github.com/linguohua/titan/node/modules"
	"github.com/linguohua/titan/node/repo"
	"github.com/linguohua/titan/node/scheduler"
	"github.com/linguohua/titan/node/scheduler/carfile"
	"github.com/linguohua/titan/node/scheduler/db/cache"
	"github.com/linguohua/titan/node/scheduler/db/persistent"
	"github.com/linguohua/titan/node/scheduler/election"
	"github.com/linguohua/titan/node/scheduler/node"
	"github.com/linguohua/titan/node/scheduler/sync"
	"github.com/linguohua/titan/node/scheduler/validator"
	"github.com/linguohua/titan/node/scheduler/web"
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
			s.invokes[ExtractApiKey] = fx.Populate(resAPI)
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
		Override(new(*config.SchedulerCfg), cfg),
		Override(new(*sqlx.DB), modules.NewDB(cfg)),
		Override(new(*persistent.CarfileDB), persistent.NewCarfileDB),
		Override(new(*persistent.NodeMgrDB), persistent.NewNodeMgrDB),
		Override(new(*redis.Client), modules.NewRedis(cfg)),
		Override(new(*cache.CarfileCache), cache.NewCarfileCache),
		Override(new(*cache.NodeMgrCache), cache.NewNodeManagerCache),
		Override(new(*node.Manager), node.NewManager),
		Override(new(node.ExitCallbackFunc), node.NewExitCallbackFunc),
		Override(new(*common.CommonAPI), common.NewCommonAPI),
		Override(new(common.SessionCallbackFunc), modules.NewSessionCallbackFunc),
		Override(new(common.PermissionWriteToken), modules.NewPermissionWriteToken),
		Override(new(common.PermissionAdminToken), modules.NewPermissionAdminToken),
		Override(new(*carfile.Manager), carfile.NewManager),
		Override(new(*sync.DataSync), sync.NewDataSync),
		If(cfg.EnableValidate,
			Override(new(*validator.Validator), validator.NewValidator),
		),
		Override(new(api.Web), web.NewWeb),
		Override(new(*election.Election), election.NewElection),
		Override(new(*scheduler.AppUpdater), scheduler.NewAppUpdater),
	)
}
