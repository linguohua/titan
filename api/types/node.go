package types

import "time"

// DownloadRecordInfo node download record
type DownloadRecordInfo struct {
	ID           string    `json:"-"`
	NodeID       string    `json:"node_id" db:"node_id"`
	BlockCID     string    `json:"block_cid" db:"block_cid"`
	CarfileCID   string    `json:"carfile_cid" db:"carfile_cid"`
	BlockSize    int       `json:"block_size" db:"block_size"`
	Speed        int64     `json:"speed" db:"speed"`
	Reward       int64     `json:"reward" db:"reward"`
	Status       int       `json:"status" db:"status"`
	FailedReason string    `json:"failed_reason" db:"failed_reason"`
	ClientIP     string    `json:"client_ip" db:"client_ip"`
	CreatedTime  time.Time `json:"created_time" db:"created_time"`
	CompleteTime time.Time `json:"complete_time" db:"complete_time"`
}

// NodeAllocateInfo Node Allocate Info
type NodeAllocateInfo struct {
	ID         int
	NodeID     string `db:"node_id"`
	Secret     string `db:"secret"`
	CreateTime string `db:"create_time"`
	NodeType   int    `db:"node_type"`
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

type UserBlockDownloadResult struct {
	// serial number
	SN int64
	// user signature
	Sign    []byte
	Succeed bool
}

// DownloadInfo download info of edge
type DownloadInfo struct {
	URL      string
	Sign     string
	SN       int64
	SignTime int64
	TimeOut  int
	Weight   int
	NodeID   string `json:"-"`
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

// ListNodesRsp list node
type ListNodesRsp struct {
	Data  []*NodeInfo `json:"data"`
	Total int64       `json:"total"`
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

// ListDownloadRecordRsp download record rsp
type ListDownloadRecordRsp struct {
	Data  []DownloadRecordInfo `json:"data"`
	Total int64                `json:"total"`
}

// ListValidatedResultRsp list validated result
type ListValidatedResultRsp struct {
	Total                int                   `json:"total"`
	ValidatedResultInfos []ValidatedResultInfo `json:"validate_result_infos"`
}

// ValidatedResultInfo validator result
type ValidatedResultInfo struct {
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
	// ValidateStatusBlockFail status
	ValidateStatusBlockFail
	// ValidateStatusOther status
	ValidateStatusOther
)
