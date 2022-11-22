package api

import (
	"context"
)

type Web interface {
	// cursor: start index, count: load number of device
	ListDevices(ctx context.Context, cursor int, count int) ([]WebDevice, error)
	GetDeviceInfoByID(ctx context.Context, deviceID string) (Device, error)
	ListDownloadInfo(ctx context.Context, req ListDownloadInfoReq) ([]DownloadBlockStat, error)
	ListCaches(ctx context.Context, req ListCachesReq) ([]WebCarfile, error)
	StatCaches(ctx context.Context, req ListCachesReq) (StatCachesRsp, error)
	ListDeviceConnectionLog(ctx context.Context, cursor int, count int) ([]DeviceConnectionLog, error)

	// cache manager
	AddCacheTask(ctx context.Context, carFileCID string, reliability int) error
	ListCacheTask(ctx context.Context, cursor int, count int) (DataListInfo, error)
	GetCacheTaskInfo(ctx context.Context, carFileCID string) (CacheDataInfo, error)
	CancelCacheTask(ctx context.Context, carFileCID string) error

	GetCarfileByCID(ctx context.Context, carFileCID string) (WebCarfile, error)
	GetBlocksByCarfileCID(ctx context.Context, carFileCID string) ([]WebBlock, error)
	RemoveCarfile(ctx context.Context, carFileCID string) error

	ListValidators(ctx context.Context, cursor int, count int) (ListValidatorsRsp, error)
	ListVadiateResult(ctx context.Context, cursor int, count int) ([]WebValidateResult, error)
	SetupValidation(ctx context.Context, DeviceID string) error
}

type WebDevice struct {
	DeviceID   string
	DeviceName string
}

type ListDownloadInfoReq struct {
	DeviceID string
	// Unix timestamp
	StartTime int64
	// Unix timestamp
	EndTime int64
	Cursor  int
	count   int
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

type DeviceConnectionLog struct {
	DeviceID    string
	OnlineTime  int64
	OfflineTime int64
}

type WebBlock struct {
	CID       string
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
