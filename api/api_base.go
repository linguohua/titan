package api

import "context"

type Base interface {
	WaitQuiet(ctx context.Context) error //perm:read
	// cache blocks
	CacheBlocks(ctx context.Context, req ReqCacheData) error //perm:read
	// told to scheduleer local block was delete
	AnnounceBlocksWasDelete(ctx context.Context, cids []string) ([]BlockOperationResult, error) //perm:read
	// delete blocks
	DeleteBlocks(ctx context.Context, cid []string) ([]BlockOperationResult, error) //perm:read
	// load block
	LoadBlock(ctx context.Context, cid string) ([]byte, error) //perm:read
	// block store stat
	BlockStoreStat(ctx context.Context) error //perm:read

	// query block cache stat
	QueryCacheStat(ctx context.Context) (CacheStat, error) //perm:read
	// query block caching stat
	QueryCachingBlocks(ctx context.Context) (CachingBlockList, error) //perm:read
	// set download server upload speed
	SetDownloadSpeed(ctx context.Context, speed int64) error //perm:read
	// generate token for user to download block
	GenerateDownloadToken(ctx context.Context) (string, error) //perm:read
}

type ReqCacheData struct {
	Cids         []string
	CandidateURL string
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
