package api

import (
	"context"
	"time"
)

type Web interface {
	// ListNodes cursor: start index, count: load number of device
	ListNodes(ctx context.Context, cursor int, count int) (ListNodesRsp, error)                                //perm:read
	GetNodeInfoByID(ctx context.Context, nodeID string) (DevicesInfo, error)                                   //perm:read
	ListNodeConnectionLog(ctx context.Context, req ListNodeConnectionLogReq) (ListNodeConnectionLogRsp, error) //perm:read
	ListBlockDownloadInfo(ctx context.Context, req ListBlockDownloadInfoReq) (ListBlockDownloadInfoRsp, error) //perm:read

	// ListCaches cache manager
	GetCacheTaskInfos(ctx context.Context, req ListCacheInfosReq) (ListCacheInfosRsp, error)          //perm:read
	GetSystemInfo(ctx context.Context) (BaseInfo, error)                                              //perm:read
	AddCacheTask(ctx context.Context, carFileCID string, reliability int, expireTime time.Time) error //perm:read
	ListCacheTasks(ctx context.Context, cursor int, count int) (ListCacheTasksRsp, error)             //perm:read
	GetCacheTaskInfo(ctx context.Context, carFileCID string) (CarfileRecordInfo, error)               //perm:read
	CancelCacheTask(ctx context.Context, carFileCID string) error                                     //perm:read

	GetCarfileByCID(ctx context.Context, carFileCID string) (WebCarfile, error) //perm:read
	RemoveCarfile(ctx context.Context, carFileCID string) error                 //perm:read

	ListValidateResult(ctx context.Context, cursor int, count int) (ListValidateResultRsp, error) //perm:read
	SetupValidation(ctx context.Context, enable bool) error                                       //perm:read

	GetSummaryValidateMessage(ctx context.Context, startTime, endTime time.Time, pageNumber, pageSize int) (*SummeryValidateResult, error) //perm:read
}

type ListNodesRsp struct {
	Data  []DevicesInfo `json:"data"`
	Total int64         `json:"total"`
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
	Datas []*CacheTaskInfo `json:"data"`
	Total int64            `json:"total"`
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

type BaseInfo struct {
	CarFileCount     int   `json:"car_file_count" redis:"CarfileCount"`
	DownloadCount    int   `json:"download_count" redis:"DownloadCount"`
	NextElectionTime int64 `json:"next_election_time" redis:"NextElectionTime"`
}

type NodeConnectionStatus int

const (
	NodeConnectionStatusOnline NodeConnectionStatus = iota + 1
	NodeConnectionStatusOffline
)

type ListNodeConnectionLogReq struct {
	NodeID string `json:"node_id"`
	// Unix timestamp
	StartTime int64 `json:"start_time"`
	// Unix timestamp
	EndTime int64 `json:"end_time"`
	Cursor  int   `json:"cursor"`
	Count   int   `json:"count"`
}

type ListNodeConnectionLogRsp struct {
	Data  []NodeConnectionLog `json:"data"`
	Total int64               `json:"total"`
}

type ListBlockDownloadInfoRsp struct {
	Data  []BlockDownloadInfo `json:"data"`
	Total int64               `json:"total"`
}

type NodeConnectionLog struct {
	DeviceID  string               `json:"device_id" db:"device_id"`
	Status    NodeConnectionStatus `json:"status" db:"status"`
	CreatedAt time.Time            `json:"created_at" db:"created_time"`
}

type WebBlock struct {
	Cid       string `json:"cid"`
	DeviceID  string `json:"device_id"`
	BlockSize int    `json:"block_size"`
}

type ListCacheTasksRsp struct {
	Data  []CarfileRecordInfo `json:"data"`
	Total int64               `json:"total"`
}

type ValidationInfo struct {
	Validators []string `json:"validators"`
	// Unix timestamp
	NextElectionTime int64 `json:"next_election_time"`
	EnableValidation bool  `json:"enable_validation"`
}

type ListValidateResultRsp struct {
	Data  []WebValidateResult `json:"data"`
	Total int64               `json:"total"`
}

type WebValidateResult struct {
	ID          int    `json:"-"`
	RoundID     string `json:"round_id" db:"round_id"`
	DeviceID    string `json:"device_id" db:"device_id"`
	ValidatorID string `json:"validator_id" db:"validator_id"`
	Msg         string `json:"msg" db:"msg"`
	Status      int    `json:"status" db:"status"`
	StartTime   string `json:"start_time" db:"start_time"`
	EndTime     string `json:"end_time" db:"end_time"`
	ServerName  string `json:"server_name" db:"server_name"`
}

type ValidateResultInfo struct {
	DeviceID      string    `db:"device_id"`
	ValidatorID   string    `db:"validator_id"`
	BlockNumber   int64     `db:"block_number"`
	Status        int       `db:"status"`
	ValidateTime  time.Time `db:"validate_time"`
	Duration      int64     `db:"duration"`
	UploadTraffic float64   `db:"upload_traffic"`
}

type SummeryValidateResult struct {
	Total               int                  `json:"total"`
	ValidateResultInfos []ValidateResultInfo `json:"validate_result_infos"`
}
