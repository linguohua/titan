package modules

import (
	"context"

	"github.com/filecoin-project/go-jsonrpc/auth"
	"github.com/jmoiron/sqlx"
	"github.com/linguohua/titan/node/common"
	"github.com/linguohua/titan/node/config"
	"github.com/linguohua/titan/node/scheduler/db/persistent"
	"github.com/linguohua/titan/node/scheduler/node"
)

func NewDB(cfg *config.SchedulerCfg) func() (*sqlx.DB, error) {
	return func() (db *sqlx.DB, err error) {
		return persistent.NewDB(cfg.PersistentDBURL)
	}
}

func GenerateTokenWithPermission(permission []auth.Permission) func(ca *common.CommonAPI) ([]byte, error) {
	return func(ca *common.CommonAPI) ([]byte, error) {
		token, err := ca.AuthNew(context.Background(), permission)
		if err != nil {
			return nil, err
		}
		return token, nil
	}
}

func NewSessionCallbackFunc(nodeMgr *node.Manager) (common.SessionCallbackFunc, error) {
	return node.NewSessionCallBackFunc(nodeMgr)
}
