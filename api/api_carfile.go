package api

import (
	"context"

	"github.com/linguohua/titan/api/types"
)

type CarfileOperation interface {
	// cache storage
	CacheCarfile(ctx context.Context, carfileCID string, sources []*types.DownloadSource) error //perm:write
	// delete storage
	DeleteCarfile(ctx context.Context, carfileCID string) error //perm:write
	// query block cache stat
	QueryCacheStat(ctx context.Context) (*types.CacheStat, error) //perm:write
	// query block caching stat
	QueryCachingCarfile(ctx context.Context) (*types.CachingAsset, error) //perm:write
	// query cache progress
	CachedProgresses(ctx context.Context, carfileCIDs []string) (*types.PullResult, error) //perm:write
}
