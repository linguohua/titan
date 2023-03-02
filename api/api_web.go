package api

import (
	"context"
	"time"
)

type Web interface {
	// ListNodes cursor: start index, count: load number of device
	ListNodes(ctx context.Context, cursor int, count int) (ListNodesRsp, error)                                //perm:read
	GetNodeInfoByID(ctx context.Context, nodeID string) (DeviceInfo, error)                                    //perm:read
	ListBlockDownloadInfo(ctx context.Context, req ListBlockDownloadInfoReq) (ListBlockDownloadInfoRsp, error) //perm:read

	// ListCaches cache manager
	GetReplicaInfos(ctx context.Context, req ListCacheInfosReq) (ListCacheInfosRsp, error) //perm:read
	GetSystemInfo(ctx context.Context) (SystemBaseInfo, error)                             //perm:read

	// ListValidateResult(ctx context.Context, cursor int, count int) (ListValidateResultRsp, error) //perm:read

	GetSummaryValidateMessage(ctx context.Context, startTime, endTime time.Time, pageNumber, pageSize int) (*SummeryValidateResult, error) //perm:read
}

type ListNodesRsp struct {
	Data  []DeviceInfo `json:"data"`
	Total int64        `json:"total"`
}

type ListBlockDownloadInfoReq struct {
	DeviceID string `json:"device_id"`
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
	DownloadCount    int   `json:"download_count" redis:"DownloadCount"`
	NextElectionTime int64 `json:"next_election_time" redis:"NextElectionTime"`
}

type NodeConnectionStatus int

const (
	NodeConnectionStatusOnline NodeConnectionStatus = iota + 1
	NodeConnectionStatusOffline
)

type ListBlockDownloadInfoRsp struct {
	Data  []BlockDownloadInfo `json:"data"`
	Total int64               `json:"total"`
}

type WebBlock struct {
	Cid       string `json:"cid"`
	DeviceID  string `json:"device_id"`
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
	Total               int              `json:"total"`
	ValidateResultInfos []ValidateResult `json:"validate_result_infos"`
}

// ValidateResult validator result
type ValidateResult struct {
	ID          int
	RoundID     int64          `db:"round_id"`
	DeviceID    string         `db:"device_id"`
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
