package types

import (
	"time"
)

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
	URL          string
	Credentials  *GatewayCredentials
	NodeID       string
	NatType      string
	SchedulerURL string
	SchedulerKey string
}

// NodeReplicaStatus node cache status
type NodeReplicaStatus struct {
	Hash   string        `db:"hash"`
	Status ReplicaStatus `db:"status"`
}

// NodeReplicaRsp node caches
type NodeReplicaRsp struct {
	Replica    []*NodeReplicaStatus
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

func (n NatType) FromString(natType string) NatType {
	switch natType {
	case "NoNat":
		return NatTypeNo
	case "SymmetricNAT":
		return NatTypeSymmetric
	case "FullConeNAT":
		return NatTypeFullCone
	case "RestrictedNAT":
		return NatTypeRestricted
	case "PortRestrictedNAT":
		return NatTypePortRestricted
	}
	return NatTypeUnknow
}

// ListNodesRsp list node
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

// ListDownloadRecordRsp download record rsp
type ListDownloadRecordRsp struct {
	Data  []DownloadRecordInfo `json:"data"`
	Total int64                `json:"total"`
}

// ListValidateResultRsp list validated result
type ListValidateResultRsp struct {
	Total                int                  `json:"total"`
	ValidatedResultInfos []ValidateResultInfo `json:"validate_result_infos"`
}

// ValidateResultInfo validator result
type ValidateResultInfo struct {
	RoundID     string         `db:"round_id"`
	NodeID      string         `db:"node_id"`
	Cid         string         `db:"cid"`
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

// Credentials gateway access credentials
type Credentials struct {
	ID        string
	NodeID    string
	CarCID    string
	ClientID  string
	LimitRate int64
	ValidTime int64
}

// GatewayCredentials be use for access gateway
type GatewayCredentials struct {
	// encrypted Credentials
	Ciphertext string
	// sign by scheduler private key
	Sign string
}

type NodeWorkloadProof struct {
	TicketID      string
	ClientID      string
	DownloadSpeed int64
	DownloadSize  int64
	StartTime     int64
	EndTime       int64
}

type NatTravelReq struct {
	Credentials *GatewayCredentials
	NodeID      string
	UserRPCURL  string
}
