package types

import (
	"time"
)

// DownloadHistory represents the record of a node download
type DownloadHistory struct {
	ID           string    `json:"-"`
	NodeID       string    `json:"node_id" db:"node_id"`
	BlockCID     string    `json:"block_cid" db:"block_cid"`
	AssetCID     string    `json:"asset_cid" db:"asset_cid"`
	BlockSize    int       `json:"block_size" db:"block_size"`
	Speed        int64     `json:"speed" db:"speed"`
	Reward       int64     `json:"reward" db:"reward"`
	Status       int       `json:"status" db:"status"`
	FailedReason string    `json:"failed_reason" db:"failed_reason"`
	ClientIP     string    `json:"client_ip" db:"client_ip"`
	CreatedTime  time.Time `json:"created_time" db:"created_time"`
	CompleteTime time.Time `json:"complete_time" db:"complete_time"`
}

// EdgeDownloadInfo download info of edge
type EdgeDownloadInfo struct {
	URL          string
	Credentials  *GatewayCredentials
	NodeID       string
	NatType      string
	SchedulerURL string
	SchedulerKey string
}

// CandidateDownloadInfo download info of candidate
type CandidateDownloadInfo struct {
	URL         string
	Credentials *GatewayCredentials
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
	Data  []DownloadHistory `json:"data"`
	Total int64             `json:"total"`
}

// ListValidationResultRsp list validated result
type ListValidationResultRsp struct {
	Total                 int                    `json:"total"`
	ValidationResultInfos []ValidationResultInfo `json:"validation_result_infos"`
}

// ValidationResultInfo validator result
type ValidationResultInfo struct {
	RoundID     string           `db:"round_id"`
	NodeID      string           `db:"node_id"`
	Cid         string           `db:"cid"`
	ValidatorID string           `db:"validator_id"`
	BlockNumber int64            `db:"block_number"` // number of blocks verified
	Status      ValidationStatus `db:"status"`
	Duration    int64            `db:"duration"` // validator duration, microsecond
	Bandwidth   float64          `db:"bandwidth"`
	StartTime   time.Time        `db:"start_time"`
	EndTime     time.Time        `db:"end_time"`

	UploadTraffic float64 `db:"upload_traffic"`
}

// ValidationStatus Validation Status
type ValidationStatus int

const (
	// ValidationStatusUnknown status
	ValidationStatusUnknown ValidationStatus = iota
	// ValidationStatusCreate status
	ValidationStatusCreate
	// ValidationStatusSuccess status
	ValidationStatusSuccess
	// ValidationStatusNodeTimeOut status
	ValidationStatusNodeTimeOut
	// ValidationStatusValidatorTimeOut status
	ValidationStatusValidatorTimeOut
	// ValidationStatusCancel status
	ValidationStatusCancel
	// ValidationStatusBlockFail status
	ValidationStatusBlockFail
	// ValidationStatusOther status
	ValidationStatusOther
)

// Credentials gateway access credentials
type Credentials struct {
	ID        string
	NodeID    string
	AssetCID  string
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

type UserProofOfWork struct {
	TicketID      string
	ClientID      string
	DownloadSpeed int64
	DownloadSize  int64
	StartTime     int64
	EndTime       int64
}

type NatPunchReq struct {
	Credentials *GatewayCredentials
	NodeID      string
	// seconds
	Timeout int
}

type ConnectOptions struct {
	Token         string
	TcpServerPort int
}
