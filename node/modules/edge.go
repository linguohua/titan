package modules

import (
	"github.com/linguohua/titan/node/carfile"
	"github.com/linguohua/titan/node/carfile/fetcher"
	"github.com/linguohua/titan/node/carfile/store"
	"github.com/linguohua/titan/node/config"
	"github.com/linguohua/titan/node/device"
	"github.com/linguohua/titan/node/modules/dtypes"
	datasync "github.com/linguohua/titan/node/sync"
	"golang.org/x/time/rate"
)

func NewDevice(bandwidthUP, bandwidthDown int64) func(nodeID dtypes.NodeID, internalIP dtypes.InternalIP, carfileStore *store.CarfileStore) *device.Device {
	return func(nodeID dtypes.NodeID, internalIP dtypes.InternalIP, carfileStore *store.CarfileStore) *device.Device {
		return device.NewDevice(string(nodeID), string(internalIP), bandwidthUP, bandwidthDown, carfileStore)
	}
}

func NewRateLimiter(device *device.Device) *rate.Limiter {
	return rate.NewLimiter(rate.Limit(device.GetBandwidthUp()), int(device.GetBandwidthUp()))
}

func NewCarfileStore(path dtypes.CarfileStorePath) (*store.CarfileStore, error) {
	return store.NewCarfileStore(string(path))
}

func NewBlockFetcherFromCandidate(cfg *config.EdgeCfg) fetcher.BlockFetcher {
	return fetcher.NewCandidate(cfg.FetchBlockTimeout, cfg.FetchBlockRetry)
}

func NewCacherForDataSync(carfileImpl *carfile.CarfileImpl) datasync.Cacher {
	return carfileImpl
}
