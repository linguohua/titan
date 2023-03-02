package modules

import (
	"context"
	"github.com/filecoin-project/go-jsonrpc/auth"
	"github.com/go-redis/redis/v8"
	"github.com/jmoiron/sqlx"
	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/node/common"
	"github.com/linguohua/titan/node/config"
	"github.com/linguohua/titan/node/scheduler/db/cache"
	"github.com/linguohua/titan/node/scheduler/db/persistent"
	"github.com/linguohua/titan/node/scheduler/node"
)

func NewDB(cfg *config.SchedulerCfg) func() (*sqlx.DB, error) {
	return func() (db *sqlx.DB, err error) {
		return persistent.NewDB(cfg.PersistentDBURL)
	}
}

func NewRedis(cfg *config.SchedulerCfg) func() (*redis.Client, error) {
	return func() (*redis.Client, error) {
		return cache.NewRedis(cfg.RedisAddress)
	}
}

func NewPermissionWriteToken(ca *common.CommonAPI) (common.PermissionWriteToken, error) {
	token, err := ca.AuthNew(context.Background(), []auth.Permission{api.PermRead, api.PermWrite})
	if err != nil {
		return nil, err
	}
	return token, nil
}

func NewPermissionAdminToken(ca *common.CommonAPI) (common.PermissionAdminToken, error) {
	token, err := ca.AuthNew(context.Background(), api.AllPermissions)
	if err != nil {
		return nil, err
	}

	return token, nil
}

func NewSessionCallbackFunc(nodeMgr *node.Manager) (common.SessionCallbackFunc, error) {
	return node.NewSessionCallBackFunc(nodeMgr)
}
