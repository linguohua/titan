package modules

import (
	"github.com/linguohua/titan/node/carfile/carfilestore"
	"github.com/linguohua/titan/node/carfile/downloader"
	"github.com/linguohua/titan/node/config"
	"github.com/linguohua/titan/node/device"
	"github.com/linguohua/titan/node/modules/dtypes"
	"go.uber.org/fx"
	"golang.org/x/time/rate"
)

type DeviceParams struct {
	fx.In

	DeviceID      dtypes.DeviceID
	InternalIP    dtypes.InternalIP
	CarfileStore  *carfilestore.CarfileStore
	BandwidthUP   int64
	BandwidthDown int64
}

func NewDevice(bandwidthUP, bandwidthDown int64) func(deviceID dtypes.DeviceID, internalIP dtypes.InternalIP, carfileStore *carfilestore.CarfileStore) *device.Device {
	return func(deviceID dtypes.DeviceID, internalIP dtypes.InternalIP, carfileStore *carfilestore.CarfileStore) *device.Device {
		return device.NewDevice(string(deviceID), string(internalIP), bandwidthUP, bandwidthDown, carfileStore)
	}
}

func NewRateLimiter(device *device.Device) *rate.Limiter {
	return rate.NewLimiter(rate.Limit(device.GetBandwidthUp()), int(device.GetBandwidthUp()))
}

func NewIPFSDownloadBlocker(cfg *config.CandidateCfg, carfileStore *carfilestore.CarfileStore) downloader.DownloadBlockser {
	log.Info("ipfs-api " + cfg.IpfsApiURL)
	return downloader.NewIPFS(cfg.IpfsApiURL, carfileStore)
}

func NewCarfileStore(storeType dtypes.CarfileStoreType, path dtypes.CarfileStorePath) *carfilestore.CarfileStore {
	return carfilestore.NewCarfileStore(string(path), string(storeType))
}

func NewCandidateDownloadBlocker(carfileStore *carfilestore.CarfileStore) downloader.DownloadBlockser {
	return downloader.NewCandidate(carfileStore)
}
