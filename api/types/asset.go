package types

import (
	"time"

	"github.com/linguohua/titan/node/modules/dtypes"
)

// ListAssetRecordRsp Data List Info
type ListAssetRecordRsp struct {
	Page      int
	TotalPage int
	Cids      int
	records   []*AssetRecord
}

// AssetCacheProgress cache asset progress
type AssetCacheProgress struct {
	CID             string
	Status          ReplicaStatus
	Msg             string
	BlocksCount     int
	DoneBlocksCount int
	Size            int64
	DoneSize        int64
}

// CacheResult cache data result info
type CacheResult struct {
	Progresses       []*AssetCacheProgress
	DiskUsage        float64
	TotalBlocksCount int
	AssetCount       int
}

// RemoveAssetResult remove asset result
type RemoveAssetResult struct {
	BlocksCount int
	DiskUsage   float64
}

// AssetRecord Asset record info
type AssetRecord struct {
	CID                   string          `db:"cid"`
	Hash                  string          `db:"hash"`
	NeedEdgeReplica       int64           `db:"edge_replicas"`
	TotalSize             int64           `db:"total_size"`
	TotalBlocks           int64           `db:"total_blocks"`
	Expiration            time.Time       `db:"expiration"`
	CreateTime            time.Time       `db:"created_time"`
	EndTime               time.Time       `db:"end_time"`
	State                 string          `db:"state"`
	NeedCandidateReplicas int64           `db:"candidate_replicas"`
	ServerID              dtypes.ServerID `db:"scheduler_sid"`

	ReplicaInfos []*ReplicaInfo
	EdgeReplica  int64
}

// ReplicaInfo asset Replica Info
type ReplicaInfo struct {
	Hash        string        `db:"hash"`
	NodeID      string        `db:"node_id"`
	Status      ReplicaStatus `db:"status"`
	IsCandidate bool          `db:"is_candidate"`
	EndTime     time.Time     `db:"end_time"`
	DoneSize    int64         `db:"done_size"`
}

// PullAssetReq pull asset to titan req
type PullAssetReq struct {
	ID         string
	CID        string
	Hash       string
	Replicas   int64
	ServerID   string
	Expiration time.Time
}

// ReplicaStatus replica pull Status
type ReplicaStatus int

const (
	// ReplicaStatusWaiting status
	ReplicaStatusWaiting ReplicaStatus = iota
	// ReplicaStatusPulling status
	ReplicaStatusPulling
	// ReplicaStatusFailed status
	ReplicaStatusFailed
	// ReplicaStatusSucceeded status
	ReplicaStatusSucceeded
)

// String status to string
func (c ReplicaStatus) String() string {
	switch c {
	case ReplicaStatusWaiting:
		return "Waiting"
	case ReplicaStatusFailed:
		return "Failed"
	case ReplicaStatusPulling:
		return "Caching"
	case ReplicaStatusSucceeded:
		return "Succeeded"
	default:
		return "Unknown"
	}
}

// CacheStatusAll all cache status
var CacheStatusAll = []string{
	ReplicaStatusWaiting.String(),
	ReplicaStatusPulling.String(),
	ReplicaStatusFailed.String(),
	ReplicaStatusSucceeded.String(),
}

// ListReplicaInfosReq list asset replicas
type ListReplicaInfosReq struct {
	// Unix timestamp
	StartTime int64 `json:"start_time"`
	// Unix timestamp
	EndTime int64 `json:"end_time"`
	Cursor  int   `json:"cursor"`
	Count   int   `json:"count"`
}

// ListReplicaInfosRsp list asset replica
type ListReplicaInfosRsp struct {
	Replicas []*ReplicaInfo `json:"data"`
	Total    int64          `json:"total"`
}

type DownloadSource struct {
	CandidateAddr string
	Credentials   *GatewayCredentials
}

type CacheStat struct {
	TotalAssetCount     int
	TotalBlockCount     int
	WaitCacheAssetCount int
	CachingAssetCID     string
	DiskUsage           float64
}

type CachingAsset struct {
	CID       string
	TotalSize int64
	DoneSize  int64
}

// AssetHash is an identifier for a asset.
type AssetHash string

func (c AssetHash) String() string {
	return string(c)
}
