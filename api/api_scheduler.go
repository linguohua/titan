package api

import (
	"context"
)

type Scheduler interface {
	Common

	// call by command
	CacheBlocks(context.Context, []string, string) ([]string, error)              //perm:read
	DeleteBlocks(context.Context, string, []string) (map[string]string, error)    //perm:read
	GetOnlineDeviceIDs(context.Context, NodeTypeName) ([]string, error)           //perm:read
	ElectionValidators(context.Context) error                                     //perm:read
	Validate(context.Context) error                                               //perm:read
	InitNodeDeviceIDs(context.Context) error                                      //perm:read
	QueryCacheStatWithNode(context.Context, string) ([]CacheStat, error)          //perm:read
	QueryCachingBlocksWithNode(context.Context, string) (CachingBlockList, error) //perm:read

	// call by node
	EdgeNodeConnect(context.Context, string) error                                   //perm:read
	DeleteBlockRecords(context.Context, string, []string) (map[string]string, error) //perm:read
	ValidateBlockResult(context.Context, ValidateResults) error                      //perm:read
	CandidateNodeConnect(context.Context, string) error                              //perm:read
	CacheResult(context.Context, string, CacheResultInfo) (string, error)            //perm:read

	// call by user
	FindNodeWithBlock(context.Context, string, string) (string, error)                            //perm:read
	GetDownloadInfoWithBlocks(context.Context, []string, string) (map[string]DownloadInfo, error) //perm:read
}

// CacheResultInfo cache data result info
type CacheResultInfo struct {
	Cid           string
	IsOK          bool
	Msg           string
	From          string
	DownloadSpeed float32
}
