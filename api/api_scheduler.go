package api

import (
	"context"
	"time"

	"github.com/filecoin-project/go-jsonrpc/auth"
	"github.com/linguohua/titan/api/types"
)

// Scheduler Scheduler node
type Scheduler interface {
	Common

	// node
	OnlineNodeList(ctx context.Context, nodeType types.NodeType) ([]string, error)                            //perm:read
	AllocateNodes(ctx context.Context, nodeType types.NodeType, count int) ([]*types.NodeAllocateInfo, error) //perm:admin
	NodeQuit(ctx context.Context, nodeID, secret string) error                                                //perm:admin
	NodeLogFileInfo(ctx context.Context, nodeID string) (*LogFile, error)                                     //perm:admin
	NodeLogFile(ctx context.Context, nodeID string) ([]byte, error)                                           //perm:admin
	DeleteNodeLogFile(ctx context.Context, nodeID string) error                                               //perm:admin
	SetNodePort(ctx context.Context, nodeID, port string) error                                               //perm:admin
	LocatorConnect(ctx context.Context, locatorID, locatorToken string) error                                 //perm:write
	// node send result when user download block complete
	UserDownloadResult(ctx context.Context, result types.UserDownloadResult) error                       //perm:write
	EdgeNodeConnect(ctx context.Context) error                                                           //perm:write
	NodeValidatedResult(ctx context.Context, validateResult ValidatedResult) error                       //perm:write
	CandidateNodeConnect(ctx context.Context) error                                                      //perm:write
	CacheResult(ctx context.Context, resultInfo types.CacheResult) error                                 //perm:write
	RemoveCarfileResult(ctx context.Context, resultInfo types.RemoveCarfileResult) error                 //perm:write
	NodeExternalAddr(ctx context.Context) (string, error)                                                //perm:read
	NodePublicKey(ctx context.Context) (string, error)                                                   //perm:write
	AuthNodeVerify(ctx context.Context, token string) ([]auth.Permission, error)                         //perm:read
	AuthNodeNew(ctx context.Context, perms []auth.Permission, nodeID, nodeSecret string) ([]byte, error) //perm:read
	NodeInfo(ctx context.Context, nodeID string) (*types.NodeInfo, error)                                //perm:read
	NodeList(ctx context.Context, cursor int, count int) (*types.ListNodesRsp, error)                    //perm:read
	// nat travel, can get edge external addr with different scheduler
	EdgeExternalAddr(ctx context.Context, nodeID, schedulerURL string) (string, error) //perm:write
	// nat travel
	IsBehindFullConeNAT(ctx context.Context, edgeURL string) (bool, error) //perm:read
	NodeNatType(ctx context.Context, nodeID string) (types.NatType, error) //perm:write
	// user
	EdgeDownloadInfos(ctx context.Context, cid string) ([]*types.DownloadInfo, error) //perm:read

	// carfile
	CacheCarfiles(ctx context.Context, info *types.CacheCarfileInfo) error               //perm:admin
	CarfileStatus(ctx context.Context, cid types.CarfileID) (types.CarfileInfo, error)   // perm:read
	RemoveCarfile(ctx context.Context, carfileID string) error                           //perm:admin
	RemoveReplica(ctx context.Context, carfileID, nodeID string) error                   //perm:admin
	CarfileRecord(ctx context.Context, cid string) (*types.CarfileRecordInfo, error)     //perm:read
	CarfileRecords(ctx context.Context, page int) (*types.ListCarfileRecordRsp, error)   //perm:read
	DownloadingCarfileRecords(ctx context.Context) ([]*types.CarfileRecordInfo, error)   //perm:read
	ResetCarfileExpiration(ctx context.Context, carfileCid string, time time.Time) error //perm:admin
	ResetCandidateReplicaCount(ctx context.Context, count int) error                     //perm:admin
	RecacheCarfiles(ctx context.Context, hashs []string) error                           //perm:admin

	// server
	StartOnceElection(ctx context.Context) error                                   //perm:admin
	ValidationEnable(ctx context.Context) (bool, error)                            //perm:admin
	StartOnceValidate(ctx context.Context) error                                   //perm:admin
	GetNodeAppUpdateInfos(ctx context.Context) (map[int]*NodeAppUpdateInfo, error) //perm:read
	SetNodeAppUpdateInfo(ctx context.Context, info *NodeAppUpdateInfo) error       //perm:admin
	DeleteNodeAppUpdateInfos(ctx context.Context, nodeType int) error              //perm:admin

	// user send result when user download block complete or failed
	UserDownloadBlockResults(ctx context.Context, results []types.UserBlockDownloadResult) error //perm:read
	// NodeList cursor: start index, count: load number of node
	DownloadRecordList(ctx context.Context, req types.ListBlockDownloadInfoReq) (*types.ListDownloadRecordRsp, error) //perm:read
	// ListCaches cache manager
	CarfileReplicaList(ctx context.Context, req types.ListCacheInfosReq) (*types.ListCarfileReplicaRsp, error)                              //perm:read
	SystemInfo(ctx context.Context) (types.SystemBaseInfo, error)                                                                           //perm:read
	ValidatedResultList(ctx context.Context, startTime, endTime time.Time, pageNumber, pageSize int) (*types.ListValidatedResultRsp, error) //perm:read
}
