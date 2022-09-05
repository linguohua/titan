package api

import "context"

type Base interface {
	WaitQuiet(ctx context.Context) error //perm:read
	// cache blocks
	CacheBlocks(ctx context.Context, req ReqCacheData) error //perm:read
	// TODO: 去掉
	DeleteData(ctx context.Context, cids []string) (DelResult, error) //perm:read
	// delete blocks
	DeleteBlocks(ctx context.Context, cid []string) (DelResult, error) //perm:read

	LoadData(ctx context.Context, cid string) ([]byte, error) //perm:read
	BlockStoreStat(ctx context.Context) error                 //perm:read

	// query block cache stat
	QueryCacheStat(ctx context.Context) (CacheStat, error) //perm:read
	// query block caching stat
	QueryCachingBlocks(ctx context.Context) (CachingBlockList, error) //perm:read

	SetDownloadSpeed(ctx context.Context, speed int64) error   //perm:read
	UnlimitDownloadSpeed(ctx context.Context) error            //perm:read
	GenerateDownloadToken(ctx context.Context) (string, error) //perm:read
}

type ReqCacheData struct {
	Cids         []string
	CandidateURL string
}

type DelFailed struct {
	Cid    string
	ErrMsg string
}

type DelResult struct {
	List []DelFailed
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
