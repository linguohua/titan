package api

import "context"

type CarfileOperation interface {
	// cache carfile
	CacheCarfile(ctx context.Context, carfileCID string, sources []*DowloadSource) (*CacheCarfileResult, error) //perm:write
	// delete carfile
	DeleteCarfile(ctx context.Context, carfileCID string) error //perm:write
	// delete all carfiles
	DeleteAllCarfiles(ctx context.Context) error //perm:admin
	// delete carfile with wait cache
	DeleteWaitCacheCarfile(ctx context.Context, carfileCID string) (blockCount int, err error) //perm:admin

	// query block cache stat
	QueryCacheStat(ctx context.Context) (*CacheStat, error) //perm:write
	// query block caching stat
	QueryCachingCarfile(ctx context.Context) (*CachingCarfile, error) //perm:write

}

type CacheCarfileResult struct {
	CacheCarfileCount   int
	WaitCacheCarfileNum int
	// DoingCacheCarfileNum int
	DiskUsage float64
}

type DowloadSource struct {
	CandidateURL   string
	CandidateToken string
}

type CacheStat struct {
	TotalCarfileCount     int
	TotalBlockCount       int
	WaitCacheCarfileCount int
	CachingCarfileCID     string
	DiskUsage             float64
}

type CachingCarfile struct {
	CarfileCID string
	BlockList  []string
}
