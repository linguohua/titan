package api

import (
	"context"
	"time"

	"github.com/filecoin-project/go-jsonrpc/auth"
)

// Scheduler Scheduler node
type Scheduler interface {
	Common
	Web

	// call by command
	GetOnlineNodeIDs(ctx context.Context, nodeType NodeType) ([]string, error)                   //perm:read
	ElectionValidators(ctx context.Context) error                                                //perm:admin
	CacheCarfile(ctx context.Context, info *CacheCarfileInfo) error                              //perm:admin
	RemoveCarfile(ctx context.Context, carfileID string) error                                   //perm:admin
	RemoveCache(ctx context.Context, carfileID, nodeID string) error                             //perm:admin
	GetCarfileRecordInfo(ctx context.Context, cid string) (CarfileRecordInfo, error)             //perm:read
	ListCarfileRecords(ctx context.Context, page int) (*CarfileRecordsInfo, error)               //perm:read
	GetDownloadingCarfileRecords(ctx context.Context) ([]*CarfileRecordInfo, error)              //perm:read
	AllocateNodes(ctx context.Context, nodeType NodeType, count int) ([]NodeAllocateInfo, error) //perm:admin
	ValidateSwitch(ctx context.Context, open bool) error                                         //perm:admin
	ValidateRunningState(ctx context.Context) (bool, error)                                      //perm:admin
	ValidateStart(ctx context.Context) error                                                     //perm:admin
	ResetCacheExpirationTime(ctx context.Context, carfileCid string, time time.Time) error       //perm:admin
	NodeQuit(ctx context.Context, device, secret string) error                                   //perm:admin
	ResetReplicaCacheCount(ctx context.Context, count int) error                                 //perm:admin
	ExecuteUndoneCarfilesTask(ctx context.Context, hashs []string) error                         //perm:admin
	ShowNodeLogFile(ctx context.Context, nodeID string) (*LogFile, error)                        //perm:admin
	DownloadNodeLogFile(ctx context.Context, nodeID string) ([]byte, error)                      //perm:admin
	DeleteNodeLogFile(ctx context.Context, nodeID string) error                                  //perm:admin
	SetNodePort(ctx context.Context, nodeID, port string) error                                  //perm:admin
	// call by locator
	LocatorConnect(ctx context.Context, locatorID, locatorToken string) error //perm:write

	// call by node
	// node send result when user download block complete
	NodeResultForUserDownloadBlock(ctx context.Context, result NodeBlockDownloadResult) error              //perm:write
	EdgeNodeConnect(ctx context.Context) error                                                             //perm:write
	ValidateBlockResult(ctx context.Context, validateResults ValidateResults) error                        //perm:write
	CandidateNodeConnect(ctx context.Context) error                                                        //perm:write
	CacheResult(ctx context.Context, resultInfo CacheResultInfo) error                                     //perm:write
	RemoveCarfileResult(ctx context.Context, resultInfo RemoveCarfileResultInfo) error                     //perm:write
	GetExternalAddr(ctx context.Context) (string, error)                                                   //perm:read
	GetPublicKey(ctx context.Context) (string, error)                                                      //perm:write
	AuthNodeVerify(ctx context.Context, token string) ([]auth.Permission, error)                           //perm:read
	AuthNodeNew(ctx context.Context, perms []auth.Permission, nodeID, deviceSecret string) ([]byte, error) //perm:read

	GetNodeAppUpdateInfos(ctx context.Context) (map[int]*NodeAppUpdateInfo, error) //perm:read
	SetNodeAppUpdateInfo(ctx context.Context, info *NodeAppUpdateInfo) error       //perm:admin                                                           //perm:write
	DeleteNodeAppUpdateInfos(ctx context.Context, nodeType int) error              //perm:admin

	// nat travel, can get edge external addr with different scheduler
	GetEdgeExternalAddr(ctx context.Context, nodeID, schedulerURL string) (string, error) //perm:write
	// nat travel
	CheckEdgeIfBehindFullConeNAT(ctx context.Context, edgeURL string) (bool, error) //perm:read
	GetNatType(ctx context.Context, nodeID string) (string, error)                  //perm:write

	// call by user
	GetDownloadInfosWithCarfile(ctx context.Context, cid, publicKey string) ([]*DownloadInfoResult, error) //perm:read
	GetDevicesInfo(ctx context.Context, nodeID string) (NodeInfo, error)                                   //perm:read
	GetDownloadInfo(ctx context.Context, nodeID string) ([]*BlockDownloadInfo, error)                      //perm:read

	// user send result when user download block complete or failed
	UserDownloadBlockResults(ctx context.Context, results []UserBlockDownloadResult) error //perm:read
}

// CarfileRecordsInfo Data List Info
type CarfileRecordsInfo struct {
	Page           int
	TotalPage      int
	Cids           int
	CarfileRecords []*CarfileRecordInfo
}

// CacheEventInfo Event Info
type CacheEventInfo struct {
	ID   int
	CID  string    `db:"cid"`
	Msg  string    `db:"msg"`
	Time time.Time `db:"time"`
}

// NodeRegisterInfo Node Register Info
type NodeAllocateInfo struct {
	ID         int
	NodeID     string `db:"node_id"`
	Secret     string `db:"secret"`
	CreateTime string `db:"create_time"`
	NodeType   int    `db:"node_type"`
}

// CacheResultInfo cache data result info
type CacheResultInfo struct {
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

// RemoveCarfileResultInfo remove carfile result
type RemoveCarfileResultInfo struct {
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

type NodeBlockDownloadResult struct {
	// serial number
	SN            int64
	Sign          []byte
	DownloadSpeed int64
	BlockSize     int
	Succeed       bool
	FailedReason  string
	BlockCID      string
}

type DownloadServerAccessAuth struct {
	NodeID     string
	URL        string
	PrivateKey string
}

type UserBlockDownloadResult struct {
	// serial number
	SN int64
	// user signature
	Sign    []byte
	Succeed bool
}

type DownloadInfoResult struct {
	URL      string
	Sign     string
	SN       int64
	SignTime int64
	TimeOut  int
	Weight   int
	NodeID   string `json:"-"`
}

type CandidateDownloadInfo struct {
	URL   string
	Token string
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

type NodeAppUpdateInfo struct {
	NodeType    int       `db:"node_type"`
	AppName     string    `db:"app_name"`
	Version     Version   `db:"version"`
	Hash        string    `db:"hash"`
	DownloadURL string    `db:"download_url"`
	UpdateTime  time.Time `db:"update_time"`
}

// NodeCacheStatus node cache status
type NodeCacheStatus struct {
	CarfileHash string      `db:"carfile_hash"`
	Status      CacheStatus `db:"status"`
}

// NodeCacheRsp node caches
type NodeCacheRsp struct {
	Caches     []*NodeCacheStatus
	TotalCount int
}

// CarfileRecordCacheResult cache result
type CarfileRecordCacheResult struct {
	NodeErrs             map[string]string
	ErrMsg               string
	EdgeNodeCacheSummary string
}

type NatType int

const (
	NatTypeUnknow NatType = iota
	// not bihind nat
	NatTypeNo
	NatTypeSymmetric
	NatTypeFullCone
	NatTypeRestricted
	NatTypePortRestricted
)
