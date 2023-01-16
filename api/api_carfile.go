package api

import "context"

type CarfileOperation interface {
	// cache carfile
	CacheCarfile(ctx context.Context, carfileCID string, sources []*DowloadSource) (CacheCarfileResult, error) //perm:write
	// delete carfile
	DeleteCarfile(ctx context.Context, carfileCID string) error //perm:write
	// delete all carfiles
	DeleteAllCarfiles(ctx context.Context) error //perm:admin
	// delete carfile with wait cache
	DeleteWaitCacheCarfile(ctx context.Context, carfileCID string) (blockCount int, err error) //perm:admin
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
