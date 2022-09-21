package api

import "context"

type Block interface {
	// cache blocks
	CacheBlocks(ctx context.Context, req ReqCacheData) error //perm:read
	// told to scheduler local block was delete
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

	GetCID(ctx context.Context, fid string) (string, error) //perm:read
	GetFID(ctx context.Context, cid string) (string, error) //perm:read
	DeleteAllBlocks(ctx context.Context) error              //perm:read

	// hash to check block store data consistent
	GetBlockStoreCheckSum(ctx context.Context) (string, error) //perm:read
	ScrubBlocks(ctx context.Context, scrub ScrubBlocks) error  //perm:read
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

type ScrubBlocks struct {
	// key fid, value cid
	// compare cid one by one
	Blocks   map[string]string
	StartFid string
	EndFix   string
}
