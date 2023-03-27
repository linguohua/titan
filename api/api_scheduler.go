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
	OnlineNodeList(ctx context.Context, nodeType types.NodeType) ([]string, error)             //perm:read
	RegisterNode(ctx context.Context, nodeID, publicKey string, nodeType types.NodeType) error //perm:admin
	NodeQuit(ctx context.Context, nodeID string) error                                         //perm:admin
	SetNodePort(ctx context.Context, nodeID, port string) error                                //perm:admin
	LocatorConnect(ctx context.Context, locatorID, locatorToken string) error                  //perm:write
	// node send result when user download block complete
	UserDownloadResult(ctx context.Context, result types.UserDownloadResult) error    //perm:write
	EdgeNodeConnect(ctx context.Context, token string) error                          //perm:write
	NodeValidatedResult(ctx context.Context, validateResult ValidateResult) error     //perm:write
	CandidateNodeConnect(ctx context.Context, token string) error                     //perm:write
	RemoveAssetResult(ctx context.Context, resultInfo types.RemoveAssetResult) error  //perm:write
	NodeExternalServiceAddress(ctx context.Context) (string, error)                   //perm:read
	NodeAuthVerify(ctx context.Context, token string) ([]auth.Permission, error)      //perm:read
	NodeAuthNew(ctx context.Context, nodeID, sign string) (string, error)             //perm:read
	NodeInfo(ctx context.Context, nodeID string) (types.NodeInfo, error)              //perm:read
	NodeList(ctx context.Context, cursor int, count int) (*types.ListNodesRsp, error) //perm:read
	// get scheduler public key, format is pem
	PublicKey(ctx context.Context) (string, error) //perm:write
	// nat travel, can get edge external addr with different scheduler
	EdgeExternalServiceAddress(ctx context.Context, nodeID, schedulerURL string) (string, error) //perm:write
	// nat travel
	CheckEdgeConnectivityWithRandomPort(ctx context.Context, edgeURL string) (bool, error) //perm:read
	NodeNatType(ctx context.Context, nodeID string) (types.NatType, error)                 //perm:write
	// user
	EdgeDownloadInfos(ctx context.Context, cid string) ([]*types.DownloadInfo, error)              //perm:read
	FindCandidateDownloadSources(ctx context.Context, cid string) ([]*types.DownloadSource, error) //perm:read

	// Asset
	CacheAsset(ctx context.Context, info *types.PullAssetReq) error                                     //perm:admin
	RemoveAsset(ctx context.Context, cid string) error                                                  //perm:admin
	AssetRecord(ctx context.Context, cid string) (*types.AssetRecord, error)                            //perm:read
	AssetRecords(ctx context.Context, limit, offset int, states []string) ([]*types.AssetRecord, error) //perm:read
	RestartFailedAssets(ctx context.Context, hashes []types.AssetHash) error                            //perm:admin
	ResetAssetExpiration(ctx context.Context, cid string, time time.Time) error                         //perm:admin

	// server
	StartOnceElection(ctx context.Context) error //perm:admin
	// same as EdgeUpdateInfos, support to update old version edge
	GetNodeAppUpdateInfos(ctx context.Context) (map[int]*EdgeUpdateInfo, error) //perm:read
	EdgeUpdateInfos(ctx context.Context) (map[int]*EdgeUpdateInfo, error)       //perm:read
	SetEdgeUpdateInfo(ctx context.Context, info *EdgeUpdateInfo) error          //perm:admin
	DeleteEdgeUpdateInfo(ctx context.Context, nodeType int) error               //perm:admin

	// user send result when user download block complete or failed
	UserDownloadBlockResults(ctx context.Context, results []types.UserBlockDownloadResult) error //perm:read
	// ListCaches cache manager
	AssetReplicaList(ctx context.Context, req types.ListReplicaInfosReq) (*types.ListReplicaInfosRsp, error)                               //perm:read
	ValidatedResultList(ctx context.Context, startTime, endTime time.Time, pageNumber, pageSize int) (*types.ListValidateResultRsp, error) //perm:read
	SubmitProofOfWork(ctx context.Context, proofs []*types.NodeWorkloadProof) error                                                        //perm:read
}
