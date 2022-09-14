package api

import (
	"context"
)

// Scheduler Scheduler node
type Scheduler interface {
	Common

	// call by command
	CacheBlocks(ctx context.Context, cids []string, deviceID string) ([]string, error)           //perm:read
	DeleteBlocks(ctx context.Context, deviceID string, cids []string) (map[string]string, error) //perm:read
	GetOnlineDeviceIDs(ctx context.Context, nodeType NodeTypeName) ([]string, error)             //perm:read
	ElectionValidators(ctx context.Context) error                                                //perm:read
	Validate(ctx context.Context) error                                                          //perm:read
	InitNodeDeviceIDs(ctx context.Context) error                                                 //perm:read
	QueryCacheStatWithNode(ctx context.Context, deviceID string) ([]CacheStat, error)            //perm:read
	QueryCachingBlocksWithNode(ctx context.Context, deviceID string) (CachingBlockList, error)   //perm:read

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
}

// CacheResultInfo cache data result info
type CacheResultInfo struct {
	Cid           string
	IsOK          bool
	Msg           string
	From          string
	DownloadSpeed float32
}
