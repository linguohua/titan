package modules

import (
	"context"

	logging "github.com/ipfs/go-log/v2"
	"github.com/linguohua/titan/node/config"
	"github.com/linguohua/titan/node/modules/dtypes"
	"github.com/linguohua/titan/node/modules/helpers"
	"github.com/linguohua/titan/node/repo"
	"github.com/linguohua/titan/node/scheduler/storage"
	"github.com/linguohua/titan/node/scheduler/validation"
	"go.uber.org/fx"

	"github.com/filecoin-project/go-jsonrpc/auth"
	"github.com/jmoiron/sqlx"
	"github.com/linguohua/titan/node/common"
	"github.com/linguohua/titan/node/scheduler/db/persistent"
	"github.com/linguohua/titan/node/scheduler/node"
)

var log = logging.Logger("modules")

// NewDB returns an *sqlx.DB instance
func NewDB(dbPath dtypes.DatabaseAddress) (*sqlx.DB, error) {
	return persistent.NewDB(string(dbPath))
}

// GenerateTokenWithPermission create a new token based on the given permissions
func GenerateTokenWithPermission(permission []auth.Permission) func(ca *common.CommonAPI) ([]byte, error) {
	return func(ca *common.CommonAPI) ([]byte, error) {
		token, err := ca.AuthNew(context.Background(), permission)
		if err != nil {
			return nil, err
		}
		return token, nil
	}
}

// DefaultSessionCallback ...
func DefaultSessionCallback() dtypes.SessionCallbackFunc {
	return func(s string, s2 string) {}
}

// NewSessionCallbackFunc callback function when the node sends a heartbeat
func NewSessionCallbackFunc(nodeMgr *node.Manager) (dtypes.SessionCallbackFunc, error) {
	return node.NewSessionCallBackFunc(nodeMgr)
}

type StorageManagerParams struct {
	fx.In

	Lifecycle  fx.Lifecycle
	MetricsCtx helpers.MetricsCtx
	// API        v1api.FullNode
	Token      dtypes.PermissionWriteToken
	MetadataDS dtypes.MetadataDS
	NodeManger *node.Manager
	dtypes.GetSchedulerConfigFunc
}

func NewStorageManager(params StorageManagerParams) *storage.Manager {
	var (
		mctx    = params.MetricsCtx
		lc      = params.Lifecycle
		nodeMgr = params.NodeManger
		token   = params.Token
		ds      = params.MetadataDS
		cfgFunc = params.GetSchedulerConfigFunc
	)

	ctx := helpers.LifecycleCtx(mctx, lc)
	m := storage.NewManager(nodeMgr, token, ds, cfgFunc)

	lc.Append(fx.Hook{
		OnStart: func(context.Context) error {
			go m.Run(ctx)
			return nil
		},
		OnStop: m.Stop,
	})

	return m
}

func NewValidation(mctx helpers.MetricsCtx, lc fx.Lifecycle, m *node.Manager, cfg *config.SchedulerCfg, configFunc dtypes.GetSchedulerConfigFunc) *validation.Validation {
	v := validation.New(m, configFunc)

	ctx := helpers.LifecycleCtx(mctx, lc)
	lc.Append(fx.Hook{
		OnStart: func(context.Context) error {
			go v.Start(ctx)
			return nil
		},
		OnStop: v.Stop,
	})

	return v
}

func NewSetSchedulerConfigFunc(r repo.LockedRepo) func(config.SchedulerCfg) error {
	return func(cfg config.SchedulerCfg) (err error) {
		return r.SetConfig(func(raw interface{}) {
			scfg, ok := raw.(*config.SchedulerCfg)
			if !ok {
				return
			}
			scfg.SchedulerServer1 = cfg.SchedulerServer1
			scfg.SchedulerServer2 = cfg.SchedulerServer2
			scfg.EnableValidate = cfg.EnableValidate
		})
	}
}

func NewGetSchedulerConfigFunc(r repo.LockedRepo) func() (config.SchedulerCfg, error) {
	return func() (out config.SchedulerCfg, err error) {
		raw, err := r.Config()
		if err != nil {
			return
		}

		scfg, ok := raw.(*config.SchedulerCfg)
		if !ok {
			return
		}

		out = *scfg
		return
	}
}
