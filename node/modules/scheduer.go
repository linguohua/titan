package modules

import (
	"context"
	logging "github.com/ipfs/go-log/v2"
	"github.com/linguohua/titan/node/modules/dtypes"
	"github.com/linguohua/titan/node/modules/helpers"
	"github.com/linguohua/titan/node/scheduler/storage"
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

// NewExitCallbackFunc callback function when the node exits
func NewExitCallbackFunc(cdb *persistent.CarfileDB) (dtypes.ExitCallbackFunc, error) {
	return func(nodeIDs []string) {
		log.Infof("node event , nodes quit:%v", nodeIDs)

		hashes, err := cdb.LoadCarfileRecordsWithNodes(nodeIDs)
		if err != nil {
			log.Errorf("LoadCarfileRecordsWithNodes err:%s", err.Error())
			return
		}

		err = cdb.RemoveReplicaInfoWithNodes(nodeIDs)
		if err != nil {
			log.Errorf("RemoveReplicaInfoWithNodes err:%s", err.Error())
			return
		}

		for _, hash := range hashes {
			log.Infof("need restore storage :%s", hash)
		}
	}, nil
}

type StorageManagerParams struct {
	fx.In

	Lifecycle  fx.Lifecycle
	MetricsCtx helpers.MetricsCtx
	//API        v1api.FullNode
	Token      dtypes.PermissionWriteToken
	MetadataDS dtypes.MetadataDS
	NodeManger *node.Manager
}

func NewStorageManager(params StorageManagerParams) *storage.Manager {
	var (
		mctx    = params.MetricsCtx
		lc      = params.Lifecycle
		nodeMgr = params.NodeManger
		token   = params.Token
		ds      = params.MetadataDS
	)

	ctx := helpers.LifecycleCtx(mctx, lc)
	m := storage.NewManager(nodeMgr, token, ds)

	lc.Append(fx.Hook{
		OnStart: func(context.Context) error {
			go m.Run(ctx)
			return nil
		},
		OnStop: m.Stop,
	})

	return m
}

