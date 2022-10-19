package api

import (
	"context"
)

// Scheduler Scheduler node
type Scheduler interface {
	Common

	// call by command
	CacheBlocks(ctx context.Context, cids []string, deviceID string) ([]string, error)           //perm:admin
	DeleteBlocks(ctx context.Context, deviceID string, cids []string) (map[string]string, error) //perm:admin
	GetOnlineDeviceIDs(ctx context.Context, nodeType NodeTypeName) ([]string, error)             //perm:admin
	ElectionValidators(ctx context.Context) error                                                //perm:admin
	Validate(ctx context.Context) error                                                          //perm:admin
	InitNodeDeviceIDs(ctx context.Context) error                                                 //perm:admin
	QueryCacheStatWithNode(ctx context.Context, deviceID string) ([]CacheStat, error)            //perm:admin
	QueryCachingBlocksWithNode(ctx context.Context, deviceID string) (CachingBlockList, error)   //perm:admin
	CacheCarFile(ctx context.Context, cid string, reliability int) error                         //perm:admin
	ShowDataInfos(ctx context.Context, cid string) (string, error)                               //perm:admin

	// call by node
	EdgeNodeConnect(ctx context.Context, deviceID string) error                                        //perm:read
	DeleteBlockRecords(ctx context.Context, deviceID string, cids []string) (map[string]string, error) //perm:read
	ValidateBlockResult(ctx context.Context, validateResults ValidateResults) error                    //perm:read
	CandidateNodeConnect(ctx context.Context, deviceID string) error                                   //perm:read
	CacheResult(ctx context.Context, deviceID string, resultInfo CacheResultInfo) (string, error)      //perm:read
	// call by user
	FindNodeWithBlock(ctx context.Context, cid string, ip string) (string, error)                             //perm:read
	GetDownloadInfoWithBlocks(ctx context.Context, cids []string, ip string) (map[string]DownloadInfo, error) //perm:read
	GetDownloadInfoWithBlock(ctx context.Context, cid string, ip string) (DownloadInfo, error)                //perm:read
	GetDevicesInfo(ctx context.Context, deviceID string) (DevicesInfo, error)                                 //perm:read
}

// CacheResultInfo cache data result info
type CacheResultInfo struct {
	DeviceID      string
	Cid           string
	IsOK          bool
	Msg           string
	From          string
	DownloadSpeed float32
	// links cid
	Links      []string
	BlockSize  int
	LinksSize  uint64
	CarFileCid string
}

// CacheDataInfo Cache Data Info
type CacheDataInfo struct {
	Cid             string
	NeedReliability int // 预期可靠性
	CurReliability  int // 当前可靠性
	TotalSize       int // 总大小

	CacheInfos []CacheInfo
}

// CacheInfo Cache Info
type CacheInfo struct {
	CacheID  string
	Status   int // cache 状态 1:创建 2:失败 3:成功
	DoneSize int // 已完成大小

	BloackInfo []BloackInfo
}

// BloackInfo Bloack Info
type BloackInfo struct {
	Cid      string
	Status   int    // cache 状态 1:创建 2:失败 3:成功
	DeviceID string // 在哪个设备上
	Size     int
}
