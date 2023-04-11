package modules

import (
	"github.com/linguohua/titan/node/asset"
	"github.com/linguohua/titan/node/asset/fetcher"
	"github.com/linguohua/titan/node/asset/storage"
	"github.com/linguohua/titan/node/config"
	"github.com/linguohua/titan/node/device"
	"github.com/linguohua/titan/node/modules/dtypes"
	datasync "github.com/linguohua/titan/node/sync"
	"github.com/linguohua/titan/node/validation"
	"golang.org/x/time/rate"
)

// NewDevice creates a function that generates new instances of device.Device.
func NewDevice(bandwidthUP, bandwidthDown int64) func(nodeID dtypes.NodeID, internalIP dtypes.InternalIP, storageMgr *storage.Manager) *device.Device {
	return func(nodeID dtypes.NodeID, internalIP dtypes.InternalIP, storageMgr *storage.Manager) *device.Device {
		return device.NewDevice(string(nodeID), string(internalIP), bandwidthUP, bandwidthDown, storageMgr)
	}
}

// NewRateLimiter creates a new rate limiter based on the given device's bandwidth limits.
func NewRateLimiter(device *device.Device) *rate.Limiter {
	return rate.NewLimiter(rate.Limit(device.GetBandwidthUp()), int(device.GetBandwidthUp()))
}

// NewNodeStorageManager creates a new instance of storage.Manager with the given carfile store path.
func NewNodeStorageManager(path dtypes.CarfileStorePath) (*storage.Manager, error) {
	return storage.NewManager(string(path), nil)
}

// NewAssetsManager creates a function that generates new instances of asset.Manager.
func NewAssetsManager(fetchBatch int) func(storageMgr *storage.Manager, bFetcher fetcher.BlockFetcher) (*asset.Manager, error) {
	return func(storageMgr *storage.Manager, bFetcher fetcher.BlockFetcher) (*asset.Manager, error) {
		opts := &asset.ManagerOptions{Storage: storageMgr, BFetcher: bFetcher, PullParallel: fetchBatch}
		return asset.NewManager(opts)
	}
}

// NewBlockFetcherFromCandidate creates a new instance of fetcher.BlockFetcher for the candidate network.
func NewBlockFetcherFromCandidate(cfg *config.EdgeCfg) fetcher.BlockFetcher {
	return fetcher.NewCandidateFetcher(cfg.FetchBlockTimeout, cfg.FetchBlockRetry)
}

// NewDataSync creates a new instance of datasync.DataSync with the given asset.Manager.
func NewDataSync(assetMgr *asset.Manager) *datasync.DataSync {
	return datasync.NewDataSync(assetMgr)
}

// NewNodeValidation creates a new instance of validation.Validation with the given asset.Manager and device.Device.
func NewNodeValidation(assetMgr *asset.Manager, device *device.Device) *validation.Validation {
	return validation.NewValidation(assetMgr, device)
}
