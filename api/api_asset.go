package api

import (
	"context"

	"github.com/linguohua/titan/api/types"
)

type Asset interface {
	// CacheAsset cache asset
	CacheAsset(ctx context.Context, assetCID string, sources []*types.AssetDownloadSource) error //perm:write
	// DeleteAsset delete asset
	DeleteAsset(ctx context.Context, assetCID string) error //perm:write
	// QueryCacheStat query cache stat
	QueryAssetStats(ctx context.Context) (*types.AssetStats, error) //perm:write
	// QueryCachingAsset query block caching stat
	QueryCachingAsset(ctx context.Context) (*types.InProgressAsset, error) //perm:write
	// query cache progress
	AssetProgresses(ctx context.Context, assetCIDs []string) (*types.PullResult, error) //perm:write
}
