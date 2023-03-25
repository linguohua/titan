package types

import (
	"time"

	"github.com/linguohua/titan/node/modules/dtypes"
)

// ListCarfileRecordRsp Data List Info
type ListCarfileRecordRsp struct {
	Page           int
	TotalPage      int
	Cids           int
	CarfileRecords []*AssetRecord
}

type CarfileProgress struct {
	CarfileCid         string
	Status             CacheStatus
	Msg                string
	CarfileBlocksCount int
	DoneBlocksCount    int
	CarfileSize        int64
	DoneSize           int64
}

// CacheResult cache data result info
type CacheResult struct {
	Progresses       []*CarfileProgress
	DiskUsage        float64
	TotalBlocksCount int
	CarfileCount     int
}

// RemoveCarfileResult remove carfile result
type RemoveCarfileResult struct {
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
	ServerID              dtypes.ServerID `db:"server_id"`

	ReplicaInfos []*ReplicaInfo
	EdgeReplica  int64
}

// ReplicaInfo Carfile Replica Info
type ReplicaInfo struct {
	ID          string
	Hash        string      `db:"hash"`
	NodeID      string      `db:"node_id"`
	Status      CacheStatus `db:"status"`
	IsCandidate bool        `db:"is_candidate"`
	EndTime     time.Time   `db:"end_time"`
	DoneSize    int64       `db:"done_size"`
}

// CacheAssetReq cache asset req
type CacheAssetReq struct {
	ID         string
	CID        string
	Hash       string
	Replicas   int64
	ServerID   string
	Expiration time.Time
}

// CacheStatus nodeMgrCache Status
type CacheStatus int

const (
	// CacheStatusWaiting status
	CacheStatusWaiting CacheStatus = iota
	// CacheStatusCaching status
	CacheStatusCaching
	// CacheStatusFailed status
	CacheStatusFailed
	// CacheStatusSucceeded status
	CacheStatusSucceeded
)

// String status to string
func (c CacheStatus) String() string {
	switch c {
	case CacheStatusWaiting:
		return "Waiting"
	case CacheStatusFailed:
		return "Failed"
	case CacheStatusCaching:
		return "Caching"
	case CacheStatusSucceeded:
		return "Succeeded"
	default:
		return "Unknown"
	}
}

// CacheStatusAll all cache status
var CacheStatusAll = []string{
	CacheStatusWaiting.String(),
	CacheStatusCaching.String(),
	CacheStatusFailed.String(),
	CacheStatusSucceeded.String(),
}

type ListCacheInfosReq struct {
	// Unix timestamp
	StartTime int64 `json:"start_time"`
	// Unix timestamp
	EndTime int64 `json:"end_time"`
	Cursor  int   `json:"cursor"`
	Count   int   `json:"count"`
}

// ListAssetReplicaRsp list carfile replica
type ListAssetReplicaRsp struct {
	Datas []*ReplicaInfo `json:"data"`
	Total int64          `json:"total"`
}

// SystemBaseInfo  system base info for titan
type SystemBaseInfo struct {
	CarFileCount     int   `json:"car_file_count" redis:"CarfileCount"`
	DownloadCount    int   `json:"download_blocks" redis:"DownloadCount"`
	NextElectionTime int64 `json:"next_election_time" redis:"NextElectionTime"`
}

type DownloadSource struct {
	CandidateAddr string
	Credentials   *GatewayCredentials
}

type CacheStat struct {
	TotalCarfileCount     int
	TotalBlockCount       int
	WaitCacheCarfileCount int
	CachingCarfileCID     string
	DiskUsage             float64
}

type CachingCarfile struct {
	CarfileCID string
	TotalSize  int64
	DoneSize   int64
}

// AssetHash is an identifier for a carfile.
type AssetHash string

func (c AssetHash) String() string {
	return string(c)
}
