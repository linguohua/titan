package api

import (
	"context"

	"github.com/linguohua/titan/api/types"
)

// Asset is an interface for asset manager
type Asset interface {
	// CacheAsset caches the asset with given assetCID from specified sources
	CacheAsset(ctx context.Context, assetCID string, sources []*types.AssetDownloadSource) error //perm:write
	// DeleteAsset deletes the asset with given assetCID
	DeleteAsset(ctx context.Context, assetCID string) error //perm:write
	// GetAssetStats retrieves the statistics of assets
	GetAssetStats(ctx context.Context) (*types.AssetStats, error) //perm:write
	// GetCachingAssetInfo retrieves the information of assets that are currently being cached
	GetCachingAssetInfo(ctx context.Context) (*types.InProgressAsset, error) //perm:write
	// GetAssetProgresses retrieves the progress of assets with specified assetCIDs
	GetAssetProgresses(ctx context.Context, assetCIDs []string) (*types.PullResult, error) //perm:write
}
