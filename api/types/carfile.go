package types

import "time"

// ListCarfileRecordRsp Data List Info
type ListCarfileRecordRsp struct {
	Page           int
	TotalPage      int
	Cids           int
	CarfileRecords []*CarfileRecordInfo
}

// CacheResult cache data result info
type CacheResult struct {
	Status            CacheStatus
	Msg               string
	CarfileBlockCount int
	DoneBlockCount    int
	CarfileSize       int64
	DoneSize          int64
	CarfileHash       string
	DiskUsage         float64
	TotalBlockCount   int
}

// RemoveCarfileResult remove carfile result
type RemoveCarfileResult struct {
	BlockCount int
	DiskUsage  float64
}

// CarfileRecordInfo Data info
type CarfileRecordInfo struct {
	CarfileCid   string    `db:"carfile_cid"`
	CarfileHash  string    `db:"carfile_hash"`
	Replica      int       `db:"replica"`
	TotalSize    int64     `db:"total_size"`
	TotalBlocks  int       `db:"total_blocks"`
	Expiration   time.Time `db:"expiration"`
	CreateTime   time.Time `db:"created_time"`
	EndTime      time.Time `db:"end_time"`
	ReplicaInfos []*ReplicaInfo
	ResultInfo   *CarfileRecordCacheResult
	EdgeReplica  int
}

// ReplicaInfo Carfile Replica Info
type ReplicaInfo struct {
	ID          string
	CarfileHash string      `db:"carfile_hash"`
	NodeID      string      `db:"node_id"`
	Status      CacheStatus `db:"status"`
	IsCandidate bool        `db:"is_candidate"`
	CreateTime  time.Time   `db:"created_time"`
	EndTime     time.Time   `db:"end_time"`
	DoneSize    int64
	DoneBlocks  int
}

// CacheCarfileInfo Data info
type CacheCarfileInfo struct {
	ID             string
	CarfileCid     string    `db:"carfile_cid"`
	CarfileHash    string    `db:"carfile_hash"`
	Replicas       int       `db:"replicas"`
	NodeID         string    `db:"node_id"`
	ServerID       string    `db:"server_id"`
	ExpirationTime time.Time `db:"expiration"`
}

// CacheStatus nodeMgrCache Status
type CacheStatus int

const (
	// CacheStatusCreate status
	CacheStatusCreate CacheStatus = iota
	// CacheStatusDownloading status
	CacheStatusDownloading
	// CacheStatusFailed status
	CacheStatusFailed
	// CacheStatusSucceeded status
	CacheStatusSucceeded
)

// String status to string
func (c CacheStatus) String() string {
	switch c {
	case CacheStatusCreate:
		return "create"
	case CacheStatusDownloading:
		return "download"
	case CacheStatusSucceeded:
		return "succeeded"
	default:
		return "failed"
	}
}

// CacheError cache error
type CacheError struct {
	CID       string
	Nodes     int
	SkipCount int
	DiskCount int
	Msg       string
	Time      time.Time
	NodeID    string
}

// CarfileRecordCacheResult cache result
type CarfileRecordCacheResult struct {
	NodeErrs             map[string]string
	ErrMsg               string
	EdgeNodeCacheSummary string
}

type ListCacheInfosReq struct {
	// Unix timestamp
	StartTime int64 `json:"start_time"`
	// Unix timestamp
	EndTime int64 `json:"end_time"`
	Cursor  int   `json:"cursor"`
	Count   int   `json:"count"`
}

// ListCarfileReplicaRsp list carfile replica
type ListCarfileReplicaRsp struct {
	Datas []*ReplicaInfo `json:"data"`
	Total int64          `json:"total"`
}

// SystemBaseInfo  system base info for titan
type SystemBaseInfo struct {
	CarFileCount     int   `json:"car_file_count" redis:"CarfileCount"`
	DownloadCount    int   `json:"download_blocks" redis:"DownloadCount"`
	NextElectionTime int64 `json:"next_election_time" redis:"NextElectionTime"`
}
