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
	ListCaches(ctx context.Context, req ListCachesReq) (ListCachesRsp, error)             //perm:read
	StatCaches(ctx context.Context) (StatCachesRsp, error)                                //perm:read
	AddCacheTask(ctx context.Context, carFileCID string, reliability int) error           //perm:read
	ListCacheTasks(ctx context.Context, cursor int, count int) (ListCacheTasksRsp, error) //perm:read
	GetCacheTaskInfo(ctx context.Context, carFileCID string) (CacheDataInfo, error)       //perm:read
	CancelCacheTask(ctx context.Context, carFileCID string) error                         //perm:read

	GetCarfileByCID(ctx context.Context, carFileCID string) (WebCarfile, error) //perm:read
	RemoveCarfile(ctx context.Context, carFileCID string) error                 //perm:read

	GetValidationInfo(ctx context.Context) (ValidationInfo, error)                                //perm:read
	ListValidateResult(ctx context.Context, cursor int, count int) (ListValidateResultRsp, error) //perm:read
	SetupValidation(ctx context.Context, DeviceID string) error                                   //perm:read
}

type ListNodesRsp struct {
	Data  []*DevicesInfo
	Total int64
}

type ListBlockDownloadInfoReq struct {
	DeviceID string
	// Unix timestamp
	StartTime int64
	// Unix timestamp
	EndTime int64
	Cursor  int
	Count   int
}

type ListCachesReq struct {
	// Unix timestamp
	StartTime int64
	// Unix timestamp
	EndTime int64
	Cursor  int
	count   int
}

type ListCachesRsp struct {
	Data  []WebCarfile
	Total int64
}

type WebCarfileStatus int

const (
	WebCarfileStatusAdd WebCarfileStatus = iota + 1
	WebCarfileStatusRemove
)

type WebCarfile struct {
	Cid       string
	Name      string
	Size      int64
	Status    WebCarfileStatus
	Blocks    []WebBlock
	CreatedAt time.Time
}

type StatCachesRsp struct {
	CarFileCount  int
	TotalSize     int64
	DeviceCount   int
	DownloadCount int
	HitRate       float32
}

type NodeConnectionStatus int

const (
	NodeConnectionStatusOnline NodeConnectionStatus = iota + 1
	NodeConnectionStatusOffline
)

type ListNodeConnectionLogReq struct {
	NodeID string
	// Unix timestamp
	StartTime int64
	// Unix timestamp
	EndTime int64
	Cursor  int
	Count   int
}

type ListNodeConnectionLogRsp struct {
	Data  []NodeConnectionLog
	Total int64
}

type ListBlockDownloadInfoRsp struct {
	Data  []BlockDownloadInfo
	Total int64
}

type NodeConnectionLog struct {
	DeviceID  string
	Status    NodeConnectionStatus
	CreatedAt time.Time
}

type WebBlock struct {
	Cid       string
	DeviceID  string
	BlockSize int
}

type ListCacheTasksRsp struct {
	Data  []CacheDataInfo
	Total int64
}

type ValidationInfo struct {
	Validators       []string
	NextElectionTime int64
	EnableValidation bool
}

type ListValidateResultRsp struct {
	Data  []WebValidateResult
	Total int64
}

type WebValidateResult struct {
	ID          int
	RoundID     string
	DeviceID    string
	ValidatorID string
	Msg         string
	Status      int
	StartTime   string
	EndTime     string
	ServerName  string
}
