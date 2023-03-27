package modules

import (
	"context"

	logging "github.com/ipfs/go-log/v2"
	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/api/types"
	"github.com/linguohua/titan/lib/etcdcli"
	"github.com/linguohua/titan/node/config"
	"github.com/linguohua/titan/node/modules/dtypes"
	"github.com/linguohua/titan/node/modules/helpers"
	"github.com/linguohua/titan/node/repo"
	"github.com/linguohua/titan/node/scheduler/assets"
	"github.com/linguohua/titan/node/scheduler/db"
	"github.com/linguohua/titan/node/scheduler/validation"
	"github.com/linguohua/titan/node/sqldb"
	"go.uber.org/fx"
	"golang.org/x/xerrors"

	"github.com/jmoiron/sqlx"
	"github.com/linguohua/titan/node/common"
	"github.com/linguohua/titan/node/scheduler/node"
)

var log = logging.Logger("modules")

// NewDB returns an *sqlx.DB instance
func NewDB(dbPath dtypes.DatabaseAddress) (*sqlx.DB, error) {
	return sqldb.NewDB(string(dbPath))
}

// GenerateTokenWithWritePermission create a new token based on the given permissions
func GenerateTokenWithWritePermission(ca *common.CommonAPI) (dtypes.PermissionWriteToken, error) {
	token, err := ca.AuthNew(context.Background(), api.ReadWritePerms)
	if err != nil {
		return "", err
	}
	return dtypes.PermissionWriteToken(token), nil
}

// GenerateTokenWithAdminPermission create a new token based on the given permissions
func GenerateTokenWithAdminPermission(ca *common.CommonAPI) (dtypes.PermissionAdminToken, error) {
	token, err := ca.AuthNew(context.Background(), api.AllPermissions)
	if err != nil {
		return "", err
	}
	return dtypes.PermissionAdminToken(token), nil
}

// DefaultSessionCallback ...
func DefaultSessionCallback() dtypes.SessionCallbackFunc {
	return func(s string, s2 string) {}
}

type StorageManagerParams struct {
	fx.In

	Lifecycle  fx.Lifecycle
	MetricsCtx helpers.MetricsCtx
	MetadataDS dtypes.MetadataDS
	NodeManger *node.Manager
	dtypes.GetSchedulerConfigFunc
	*db.SQLDB
}

func NewStorageManager(params StorageManagerParams) *assets.Manager {
	var (
		mctx    = params.MetricsCtx
		lc      = params.Lifecycle
		nodeMgr = params.NodeManger
		ds      = params.MetadataDS
		cfgFunc = params.GetSchedulerConfigFunc
		sdb     = params.SQLDB
	)

	ctx := helpers.LifecycleCtx(mctx, lc)
	m := assets.NewManager(nodeMgr, ds, cfgFunc, sdb)

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

// RegisterToEtcd Register server to etcd
func RegisterToEtcd(mctx helpers.MetricsCtx, lc fx.Lifecycle, configFunc dtypes.GetSchedulerConfigFunc, serverID dtypes.ServerID, token dtypes.PermissionAdminToken) error {
	cfg, err := configFunc()
	if err != nil {
		return err
	}

	sCfg := &types.SchedulerCfg{
		AreaID:       cfg.AreaID,
		SchedulerURL: cfg.ExternalAddress,
		AccessToken:  string(token),
	}

	value, err := etcdcli.SCMarshal(sCfg)
	if err != nil {
		return xerrors.Errorf("cfg SCMarshal err:%s", err.Error())
	}

	eCli, err := etcdcli.New(cfg.EtcdAddresses)
	if err != nil {
		return err
	}

	ctx := helpers.LifecycleCtx(mctx, lc)
	lc.Append(fx.Hook{
		OnStart: func(context.Context) error {
			return eCli.ServerRegister(ctx, serverID, string(value))
		},
		OnStop: func(context.Context) error {
			return eCli.ServerUnRegister(ctx, serverID)
		},
	})

	return nil
}
