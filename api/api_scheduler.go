package api

import (
	"context"
	"time"

	"github.com/filecoin-project/go-jsonrpc/auth"
	"github.com/linguohua/titan/api/types"
)

// Scheduler is an interface for scheduler
type Scheduler interface {
	Common

	// Node-related methods
	// GetOnlineNodeCount returns the count of online nodes for a given node type
	GetOnlineNodeCount(ctx context.Context, nodeType types.NodeType) (int, error) //perm:read
	// RegisterNewNode adds a new node to the scheduler with the specified node ID, public key, and node type
	RegisterNewNode(ctx context.Context, nodeID, publicKey string, nodeType types.NodeType) error //perm:admin
	// UnregisterNode removes a node from the scheduler with the specified node ID
	UnregisterNode(ctx context.Context, nodeID string) error //perm:admin
	// UpdateNodePort updates the port for the node with the specified node ID
	UpdateNodePort(ctx context.Context, nodeID, port string) error //perm:admin
	// ConnectLocator locator connected to the scheduler
	ConnectLocator(ctx context.Context, locatorID, locatorToken string) error //perm:write
	// UserDownloadResult user download result for a specific asset
	UserDownloadResult(ctx context.Context, result types.UserDownloadResult) error //perm:write
	// ConnectEdgeNode edge node locator connected to the scheduler
	ConnectEdgeNode(ctx context.Context, opts *types.ConnectOptions) error //perm:write
	// ProcessNodeValidationResult processes the validation result for a specific node
	ProcessNodeValidationResult(ctx context.Context, validateResult ValidateResult) error //perm:write
	// ConnectCandidateNode candidate node locator connected to the scheduler
	ConnectCandidateNode(ctx context.Context, opts *types.ConnectOptions) error //perm:write
	// RemoveAssetResult records the result of an asset removal operation
	RemoveAssetResult(ctx context.Context, resultInfo types.RemoveAssetResult) error //perm:write
	// GetNodeExternalAddress returns the external IP address of a node
	GetNodeExternalAddress(ctx context.Context) (string, error) //perm:read
	// VerifyNodeAuthToken checks the authenticity of a node's authentication token and returns the associated permissions
	VerifyNodeAuthToken(ctx context.Context, token string) ([]auth.Permission, error) //perm:read
	// CreateNodeAuthToken generates an authentication token for a node with the specified node ID and signature
	CreateNodeAuthToken(ctx context.Context, nodeID, sign string) (string, error) //perm:read
	// GetNodeInfo fetches information about a node with the specified node ID
	GetNodeInfo(ctx context.Context, nodeID string) (types.NodeInfo, error) //perm:read
	// GetNodeList retrieves a list of nodes with pagination using the specified cursor and count
	GetNodeList(ctx context.Context, cursor int, count int) (*types.ListNodesRsp, error) //perm:read
	// TODO GetAssetListForBucket retrieves a list of asset CIDs for a bucket associated with the specified node ID
	GetAssetListForBucket(ctx context.Context, nodeID string) ([]string, error) //perm:write
	// TODO GetEdgeExternalServiceAddress nat travel, can get edge external addr with different scheduler
	GetEdgeExternalServiceAddress(ctx context.Context, nodeID, schedulerURL string) (string, error) //perm:write
	// GetNodeNATType returns the NAT type for a node with the specified node ID
	GetNodeNATType(ctx context.Context, nodeID string) (types.NatType, error) //perm:write
	// TODO NatTravel
	NatTravel(ctx context.Context, target *types.NatTravelReq) error //perm:read
	// CheckNetworkConnectivity check tcp or udp network connectivity , network is "tcp" or "udp"
	CheckNetworkConnectivity(ctx context.Context, network, targetURL string) error //perm:read
	// GetEdgeDownloadInfos retrieves download information for the edge with the asset with the specified CID.
	GetEdgeDownloadInfos(ctx context.Context, cid string) ([]*types.DownloadInfo, error) //perm:read
	// GetCandidateDownloadSources retrieves download information for the candidate with the asset with the specified CID.
	GetCandidateDownloadSources(ctx context.Context, cid string) ([]*types.AssetDownloadSource, error) //perm:read

	// Asset-related methods
	// CacheAsset caches an scheduler in the network using the provided pull request information
	CacheAsset(ctx context.Context, info *types.PullAssetReq) error //perm:admin
	// RemoveAssetRecord removes the asset record with the specified CID from the scheduler
	RemoveAssetRecord(ctx context.Context, cid string) error //perm:admin
	// RemoveAssetReplica deletes an asset replica with the specified CID and node ID from the scheduler
	RemoveAssetReplica(ctx context.Context, cid, nodeID string) error //perm:admin
	// GetAssetRecord retrieves the asset record with the specified CID
	GetAssetRecord(ctx context.Context, cid string) (*types.AssetRecord, error) //perm:read
	// GetAssetRecords retrieves a list of asset records with pagination using the specified limit, offset, and states
	GetAssetRecords(ctx context.Context, limit, offset int, states []string) ([]*types.AssetRecord, error) //perm:read
	// RestartFailedAssets retries the download and caching process for a list of failed assets
	RestartFailedAssets(ctx context.Context, hashes []types.AssetHash) error //perm:admin
	// ResetAssetExpiration updates the expiration time for an asset with the specified CID
	ResetAssetExpiration(ctx context.Context, cid string, time time.Time) error //perm:admin
	// GetAssetReplicaList retrieves a list of asset replica information using the specified request parameters
	GetAssetReplicaList(ctx context.Context, req types.ListReplicaInfosReq) (*types.ListReplicaInfosRsp, error) //perm:read
	// GetValidationResultList retrieves a list of validation results with pagination using the specified time range, page number, and page size
	GetValidationResultList(ctx context.Context, startTime, endTime time.Time, pageNumber, pageSize int) (*types.ListValidateResultRsp, error) //perm:read
	// TODO UserDownloadBlockResults user send result when user download block complete or failed
	UserDownloadBlockResults(ctx context.Context, results []types.UserBlockDownloadResult) error //perm:read
	// TODO IgnoreProofOfWork
	IgnoreProofOfWork(ctx context.Context, proofs []*types.NodeWorkloadProof) error //perm:read

	// Server-related methods
	// GetServerPublicKey retrieves the scheduler's public key in PEM format
	GetServerPublicKey(ctx context.Context) (string, error) //perm:write
	// TriggerElection starts a new election process
	TriggerElection(ctx context.Context) error //perm:admin
	// TODO GetNodeAppUpdateInfos same as EdgeUpdateInfos, support to update old version edge
	GetNodeAppUpdateInfos(ctx context.Context) (map[int]*EdgeUpdateInfo, error) //perm:read
	// TODO
	GetEdgeUpdateInfos(ctx context.Context) (map[int]*EdgeUpdateInfo, error) //perm:read
	// TODO
	SetEdgeUpdateInfo(ctx context.Context, info *EdgeUpdateInfo) error //perm:admin
	// TODO
	DeleteEdgeUpdateInfo(ctx context.Context, nodeType int) error //perm:admin
}
