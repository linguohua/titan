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

func NewDevice(bandwidthUP, bandwidthDown int64) func(nodeID dtypes.NodeID, internalIP dtypes.InternalIP, storageMgr *storage.Manager) *device.Device {
	return func(nodeID dtypes.NodeID, internalIP dtypes.InternalIP, storageMgr *storage.Manager) *device.Device {
		return device.NewDevice(string(nodeID), string(internalIP), bandwidthUP, bandwidthDown, storageMgr)
	}
}

func NewRateLimiter(device *device.Device) *rate.Limiter {
	return rate.NewLimiter(rate.Limit(device.GetBandwidthUp()), int(device.GetBandwidthUp()))
}

func NewNodeStorageManager(path dtypes.CarfileStorePath) (*storage.Manager, error) {
	return storage.NewManager(string(path), nil)
}

func NewAssetsManager(fetchBatch int) func(storageMgr *storage.Manager, bFetcher fetcher.BlockFetcher) (*asset.Manager, error) {
	return func(storageMgr *storage.Manager, bFetcher fetcher.BlockFetcher) (*asset.Manager, error) {
		opts := &asset.ManagerOptions{Storage: storageMgr, BFetcher: bFetcher, PullParallel: fetchBatch}
		return asset.NewManager(opts)
	}
}

func NewBlockFetcherFromCandidate(cfg *config.EdgeCfg) fetcher.BlockFetcher {
	return fetcher.NewCandidate(cfg.FetchBlockTimeout, cfg.FetchBlockRetry)
}

func NewDataSync(assetMgr *asset.Manager) *datasync.DataSync {
	return datasync.NewDataSync(assetMgr)
}

func NewNodeValidation(cacheMgr *asset.Manager, device *device.Device) *validation.Validation {
	return validation.NewValidation(cacheMgr, device)
}
