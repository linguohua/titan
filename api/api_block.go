package api

import "context"

type Block interface {
	// cache blocks
	CacheBlocks(ctx context.Context, req []ReqCacheData) error //perm:write
	// told to scheduler local block was delete
	AnnounceBlocksWasDelete(ctx context.Context, cids []string) ([]BlockOperationResult, error) //perm:write
	// delete blocks
	DeleteBlocks(ctx context.Context, cid []string) ([]BlockOperationResult, error) //perm:write
	// load block
	LoadBlock(ctx context.Context, cid string) ([]byte, error) //perm:read
	// block store stat
	BlockStoreStat(ctx context.Context) error //perm:read

	// query block cache stat
	QueryCacheStat(ctx context.Context) (CacheStat, error) //perm:read
	// query block caching stat
	QueryCachingBlocks(ctx context.Context) (CachingBlockList, error) //perm:read

	GetCID(ctx context.Context, fid string) (string, error) //perm:read
	GetFID(ctx context.Context, cid string) (string, error) //perm:read
	DeleteAllBlocks(ctx context.Context) error              //perm:admin
}

type BlockInfo struct {
	Cid string
	Fid int
}

type ReqCacheData struct {
	BlockInfos    []BlockInfo
	DownloadURL   string
	DownloadToken string
	CardFileCid   string
	CacheID       string
}

type BlockOperationResult struct {
	Cid    string
	ErrMsg string
}

type CacheStat struct {
	CacheBlockCount    int
	WaitCacheBlockNum  int
	DoingCacheBlockNum int
}

type CachingBlockStat struct {
	Cid             string
	DownloadPercent float32
	DownloadSpeed   float32
	// milliseconds
	CostTime int
}

type CachingBlockList struct {
	List []CachingBlockStat
}
