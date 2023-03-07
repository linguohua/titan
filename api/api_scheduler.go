package api

import (
	"context"
	"time"

	"github.com/filecoin-project/go-jsonrpc/auth"
)

// Scheduler Scheduler node
type Scheduler interface {
	Common

	// node
	OnlineNodeList(ctx context.Context, nodeType NodeType) ([]string, error)                     //perm:read
	AllocateNodes(ctx context.Context, nodeType NodeType, count int) ([]NodeAllocateInfo, error) //perm:admin
	NodeQuit(ctx context.Context, nodeID, secret string) error                                   //perm:admin
	NodeLogFileInfo(ctx context.Context, nodeID string) (*LogFile, error)                        //perm:admin
	NodeLogFile(ctx context.Context, nodeID string) ([]byte, error)                              //perm:admin
	DeleteNodeLogFile(ctx context.Context, nodeID string) error                                  //perm:admin
	SetNodePort(ctx context.Context, nodeID, port string) error                                  //perm:admin
	LocatorConnect(ctx context.Context, locatorID, locatorToken string) error                    //perm:write
	// node send result when user download block complete
	UserDownloadResult(ctx context.Context, result UserDownloadResult) error                             //perm:write
	EdgeNodeConnect(ctx context.Context) error                                                           //perm:write
	NodeValidatedResult(ctx context.Context, validateResult ValidatedResult) error                       //perm:write
	CandidateNodeConnect(ctx context.Context) error                                                      //perm:write
	CacheResult(ctx context.Context, resultInfo CacheResult) error                                       //perm:write
	RemoveCarfileResult(ctx context.Context, resultInfo RemoveCarfileResult) error                       //perm:write
	NodeExternalAddr(ctx context.Context) (string, error)                                                //perm:read
	NodePublicKey(ctx context.Context) (string, error)                                                   //perm:write
	AuthNodeVerify(ctx context.Context, token string) ([]auth.Permission, error)                         //perm:read
	AuthNodeNew(ctx context.Context, perms []auth.Permission, nodeID, nodeSecret string) ([]byte, error) //perm:read
	NodeInfo(ctx context.Context, nodeID string) (NodeInfo, error)                                       //perm:read
	NodeDownloadRecord(ctx context.Context, nodeID string) ([]*DownloadRecordInfo, error)                //perm:read
	NodeList(ctx context.Context, cursor int, count int) (ListNodesRsp, error)                           //perm:read
	// nat travel, can get edge external addr with different scheduler
	EdgeExternalAddr(ctx context.Context, nodeID, schedulerURL string) (string, error) //perm:write
	// nat travel
	IsBehindFullConeNAT(ctx context.Context, edgeURL string) (bool, error) //perm:read
	NodeNatType(ctx context.Context, nodeID string) (NatType, error)       //perm:write
	// user
	GetDownloadInfosWithCarfile(ctx context.Context, cid string) ([]*DownloadInfoResult, error) //perm:read

	// carfile
	CacheCarfile(ctx context.Context, info *CacheCarfileInfo) error                        //perm:admin
	RemoveCarfile(ctx context.Context, carfileID string) error                             //perm:admin
	RemoveCache(ctx context.Context, carfileID, nodeID string) error                       //perm:admin
	GetCarfileRecordInfo(ctx context.Context, cid string) (CarfileRecordInfo, error)       //perm:read
	ListCarfileRecords(ctx context.Context, page int) (*CarfileRecordsInfo, error)         //perm:read
	GetDownloadingCarfileRecords(ctx context.Context) ([]*CarfileRecordInfo, error)        //perm:read
	ResetCacheExpirationTime(ctx context.Context, carfileCid string, time time.Time) error //perm:admin
	ResetReplicaCacheCount(ctx context.Context, count int) error                           //perm:admin
	ExecuteUndoneCarfilesTask(ctx context.Context, hashs []string) error                   //perm:admin

	// server
	ElectionValidators(ctx context.Context) error                                  //perm:admin
	ValidateSwitch(ctx context.Context, open bool) error                           //perm:admin
	ValidateRunningState(ctx context.Context) (bool, error)                        //perm:admin
	ValidateStart(ctx context.Context) error                                       //perm:admin
	GetNodeAppUpdateInfos(ctx context.Context) (map[int]*NodeAppUpdateInfo, error) //perm:read
	SetNodeAppUpdateInfo(ctx context.Context, info *NodeAppUpdateInfo) error       //perm:admin
	DeleteNodeAppUpdateInfos(ctx context.Context, nodeType int) error              //perm:admin

	// user send result when user download block complete or failed
	UserDownloadBlockResults(ctx context.Context, results []UserBlockDownloadResult) error //perm:read
	// NodeList cursor: start index, count: load number of node
	ListBlockDownloadInfo(ctx context.Context, req ListBlockDownloadInfoReq) (ListBlockDownloadInfoRsp, error) //perm:read
	// ListCaches cache manager
	GetReplicaInfos(ctx context.Context, req ListCacheInfosReq) (ListCacheInfosRsp, error)                                                 //perm:read
	GetSystemInfo(ctx context.Context) (SystemBaseInfo, error)                                                                             //perm:read
	GetSummaryValidateMessage(ctx context.Context, startTime, endTime time.Time, pageNumber, pageSize int) (*SummeryValidateResult, error) //perm:read
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

// UserDownloadResult user download carfile result
type UserDownloadResult struct {
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

func (n NatType) String() string {
	switch n {
	case NatTypeNo:
		return "NoNAT"
	case NatTypeSymmetric:
		return "SymmetricNAT"
	case NatTypeFullCone:
		return "FullConeNAT"
	case NatTypeRestricted:
		return "RestrictedNAT"
	case NatTypePortRestricted:
		return "PortRestrictedNAT"
	}

	return "UnknowNAT"
}

type ListNodesRsp struct {
	Data  []NodeInfo `json:"data"`
	Total int64      `json:"total"`
}

type ListBlockDownloadInfoReq struct {
	NodeID string `json:"node_id"`
	// Unix timestamp
	StartTime int64 `json:"start_time"`
	// Unix timestamp
	EndTime int64 `json:"end_time"`
	Cursor  int   `json:"cursor"`
	Count   int   `json:"count"`
}

type ListCacheInfosReq struct {
	// Unix timestamp
	StartTime int64 `json:"start_time"`
	// Unix timestamp
	EndTime int64 `json:"end_time"`
	Cursor  int   `json:"cursor"`
	Count   int   `json:"count"`
}

type ListCacheInfosRsp struct {
	Datas []*ReplicaInfo `json:"data"`
	Total int64          `json:"total"`
}

type WebCarfileStatus int

const (
	WebCarfileStatusAdd WebCarfileStatus = iota + 1
	WebCarfileStatusRemove
)

type WebCarfile struct {
	Cid       string           `json:"cid"`
	Name      string           `json:"name"`
	Size      int64            `json:"size"`
	Status    WebCarfileStatus `json:"status"`
	CreatedAt time.Time        `json:"created_at"`
}

// SystemBaseInfo  system base info for titan
type SystemBaseInfo struct {
	CarFileCount     int   `json:"car_file_count" redis:"CarfileCount"`
	DownloadCount    int   `json:"download_blocks" redis:"DownloadCount"`
	NextElectionTime int64 `json:"next_election_time" redis:"NextElectionTime"`
}

type NodeConnectionStatus int

const (
	NodeConnectionStatusOnline NodeConnectionStatus = iota + 1
	NodeConnectionStatusOffline
)

type ListBlockDownloadInfoRsp struct {
	Data  []DownloadRecordInfo `json:"data"`
	Total int64                `json:"total"`
}

type WebBlock struct {
	Cid       string `json:"cid"`
	NodeID    string `json:"node_id"`
	BlockSize int    `json:"block_size"`
}

type ListReplicasRsp struct {
	Data  []CarfileRecordInfo `json:"data"`
	Total int64               `json:"total"`
}

type ValidationInfo struct {
	Validators []string `json:"validators"`
	// Unix timestamp
	NextElectionTime int64 `json:"next_election_time"`
	EnableValidation bool  `json:"enable_validation"`
}

type SummeryValidateResult struct {
	Total               int                  `json:"total"`
	ValidateResultInfos []ValidateResultInfo `json:"validate_result_infos"`
}

// ValidateResultInfo validator result
type ValidateResultInfo struct {
	ID          int
	RoundID     string         `db:"round_id"`
	NodeID      string         `db:"node_id"`
	ValidatorID string         `db:"validator_id"`
	BlockNumber int64          `db:"block_number"` // number of blocks verified
	Status      ValidateStatus `db:"status"`
	Duration    int64          `db:"duration"` // validator duration, microsecond
	Bandwidth   float64        `db:"bandwidth"`
	StartTime   time.Time      `db:"start_time"`
	EndTime     time.Time      `db:"end_time"`

	UploadTraffic float64 `db:"upload_traffic"`
}

// ValidateStatus Validate Status
type ValidateStatus int

const (
	// ValidateStatusUnknown status
	ValidateStatusUnknown ValidateStatus = iota
	// ValidateStatusCreate status
	ValidateStatusCreate
	// ValidateStatusSuccess status
	ValidateStatusSuccess
	// ValidateStatusTimeOut status
	ValidateStatusTimeOut
	// ValidateStatusCancel status
	ValidateStatusCancel
	// ValidateStatusFail status
	ValidateStatusFail
	// ValidateStatusOther status
	ValidateStatusOther
)
