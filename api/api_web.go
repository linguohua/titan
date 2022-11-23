package api

import (
	"context"
)

type Web interface {
	// cursor: start index, count: load number of device
	ListNodes(ctx context.Context, cursor int, count int) ([]WebNode, error)                              //perm:read
	GetNodeInfoByID(ctx context.Context, nodeID string) (DevicesInfo, error)                              //perm:read
	ListNodeConnectionLog(ctx context.Context, cursor int, count int) ([]NodeConnectionLog, error)        //perm:read
	ListBlockDownloadInfo(ctx context.Context, req ListBlockDownloadInfoReq) ([]BlockDownloadInfo, error) //perm:read

	// cache manager
	ListCaches(ctx context.Context, req ListCachesReq) ([]WebCarfile, error)           //perm:read
	StatCaches(ctx context.Context) (StatCachesRsp, error)                             //perm:read
	AddCacheTask(ctx context.Context, carFileCID string, reliability int) error        //perm:read
	ListCacheTask(ctx context.Context, cursor int, count int) ([]CacheDataInfo, error) //perm:read
	GetCacheTaskInfo(ctx context.Context, carFileCID string) (CacheDataInfo, error)    //perm:read
	CancelCacheTask(ctx context.Context, carFileCID string) error                      //perm:read

	GetCarfileByCID(ctx context.Context, carFileCID string) (WebCarfile, error)       //perm:read
	GetBlocksByCarfileCID(ctx context.Context, carFileCID string) ([]WebBlock, error) //perm:read
	RemoveCarfile(ctx context.Context, carFileCID string) error                       //perm:read

	ListValidators(ctx context.Context, cursor int, count int) (ListValidatorsRsp, error)      //perm:read
	ListVadiateResult(ctx context.Context, cursor int, count int) ([]WebValidateResult, error) //perm:read
	SetupValidation(ctx context.Context, DeviceID string) error                                //perm:read
}

type WebNode struct {
	NodeID   string
	NodeName string
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

type WebCarfile struct {
	Cid  string
	Name string
	Size int64
}

type StatCachesRsp struct {
	CarFileCount  int
	TotalSize     int64
	DeviceCount   int
	DownloadCount int
	HitRate       float32
}

type NodeConnectionLog struct {
	DeviceID    string
	OnlineTime  int64
	OfflineTime int64
}

type WebBlock struct {
	Cid       string
	DeviceID  string
	BlockSize int
}

type ListValidatorsRsp struct {
	Validators       []string
	NextElectionTime int64
}

type WebValidateResult struct {
	ID          int
	RoundID     string
	DeviceID    string
	ValidatorID string
	Msg         string
	Status      int
	StratTime   string
	EndTime     string
	ServerName  string
}
