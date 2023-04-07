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
	GetOnlineNodeCount(ctx context.Context, nodeType types.NodeType) (int, error)                 //perm:read
	RegisterNewNode(ctx context.Context, nodeID, publicKey string, nodeType types.NodeType) error //perm:admin
	NodeQuit(ctx context.Context, nodeID string) error                                            //perm:admin
	UpdateNodePort(ctx context.Context, nodeID, port string) error                                //perm:admin
	ConnectLocator(ctx context.Context, locatorID, locatorToken string) error                     //perm:write
	// node send result when user download block complete
	UserDownloadResult(ctx context.Context, result types.UserDownloadResult) error        //perm:write
	ConnectEdgeNode(ctx context.Context, opts *types.ConnectOptions) error                //perm:write
	ProcessNodeValidationResult(ctx context.Context, validateResult ValidateResult) error //perm:write
	ConnectCandidateNode(ctx context.Context, opts *types.ConnectOptions) error           //perm:write
	RemoveAssetResult(ctx context.Context, resultInfo types.RemoveAssetResult) error      //perm:write
	GetNodeExternalAddress(ctx context.Context) (string, error)                           //perm:read
	VerifyNodeAuthToken(ctx context.Context, token string) ([]auth.Permission, error)     //perm:read
	CreateNodeAuthToken(ctx context.Context, nodeID, sign string) (string, error)         //perm:read
	RetrieveNodeInfo(ctx context.Context, nodeID string) (types.NodeInfo, error)          //perm:read
	GetNodeList(ctx context.Context, cursor int, count int) (*types.ListNodesRsp, error)  //perm:read
	GetAssetListForBucket(ctx context.Context, nodeID string) ([]string, error)           //perm:write
	// get scheduler public key, format is pem
	GetServerPublicKey(ctx context.Context) (string, error) //perm:write
	// nat travel, can get edge external addr with different scheduler
	EdgeExternalServiceAddress(ctx context.Context, nodeID, schedulerURL string) (string, error) //perm:write
	// CheckNetworkConnectivity check tcp or udp network connectivity
	// network is "tcp" or "udp"
	CheckNetworkConnectivity(ctx context.Context, network, targetURL string) error //perm:read
	NodeNatType(ctx context.Context, nodeID string) (types.NatType, error)         //perm:write
	// UserNatTravel request node connect back to user
	// targets is edge node list
	NatTravel(ctx context.Context, target *types.NatTravelReq) error //perm:read
	// user
	EdgeDownloadInfos(ctx context.Context, cid string) ([]*types.DownloadInfo, error)                  //perm:read
	RetrieveCandidateDownloadSources(ctx context.Context, cid string) ([]*types.DownloadSource, error) //perm:read

	// Asset
	CacheAsset(ctx context.Context, info *types.PullAssetReq) error                                     //perm:admin
	RemoveAssetRecord(ctx context.Context, cid string) error                                            //perm:admin
	RemoveAssetReplica(ctx context.Context, cid, nodeID string) error                                   //perm:admin
	AssetRecord(ctx context.Context, cid string) (*types.AssetRecord, error)                            //perm:read
	AssetRecords(ctx context.Context, limit, offset int, states []string) ([]*types.AssetRecord, error) //perm:read
	RestartFailedAssets(ctx context.Context, hashes []types.AssetHash) error                            //perm:admin
	ResetAssetExpiration(ctx context.Context, cid string, time time.Time) error                         //perm:admin

	// server
	TriggerElection(ctx context.Context) error //perm:admin
	// same as EdgeUpdateInfos, support to update old version edge
	GetNodeAppUpdateInfos(ctx context.Context) (map[int]*EdgeUpdateInfo, error) //perm:read
	EdgeUpdateInfos(ctx context.Context) (map[int]*EdgeUpdateInfo, error)       //perm:read
	SetEdgeUpdateInfo(ctx context.Context, info *EdgeUpdateInfo) error          //perm:admin
	DeleteEdgeUpdateInfo(ctx context.Context, nodeType int) error               //perm:admin

	// user send result when user download block complete or failed
	UserDownloadBlockResults(ctx context.Context, results []types.UserBlockDownloadResult) error //perm:read
	// ListCaches cache manager
	AssetReplicaList(ctx context.Context, req types.ListReplicaInfosReq) (*types.ListReplicaInfosRsp, error)                                   //perm:read
	GetValidationResultList(ctx context.Context, startTime, endTime time.Time, pageNumber, pageSize int) (*types.ListValidateResultRsp, error) //perm:read
	IgnoreProofOfWork(ctx context.Context, proofs []*types.NodeWorkloadProof) error                                                            //perm:read
}
