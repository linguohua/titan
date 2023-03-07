package api

import "context"

type CarfileOperation interface {
	// cache storage
	CacheCarfile(ctx context.Context, carfileCID string, sources []*DownloadSource) (*CacheCarfileResult, error) //perm:write
	// delete storage
	DeleteCarfile(ctx context.Context, carfileCID string) error //perm:write
	// delete all carfiles
	DeleteAllCarfiles(ctx context.Context) error //perm:admin
	// query block cache stat
	QueryCacheStat(ctx context.Context) (*CacheStat, error) //perm:write
	// query block caching stat
	QueryCachingCarfile(ctx context.Context) (*CachingCarfile, error) //perm:write
}

type CacheCarfileResult struct {
	CacheCarfileCount   int
	WaitCacheCarfileNum int
	DiskUsage           float64
}

type DownloadSource struct {
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
