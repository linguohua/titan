package modules

import (
	"github.com/linguohua/titan/node/config"
	"github.com/linguohua/titan/node/locator"
	"github.com/linguohua/titan/node/modules/dtypes"
	"github.com/linguohua/titan/region"
)

// NewRegion creates a new region instance using the given database path.
func NewRegion(dbPath dtypes.GeoDBPath) (region.Region, error) {
	return region.NewGeoLiteRegion(string(dbPath))
}

// NewLocatorStorage creates a locator storage using the give addresses
func NewLocatorStorage(cfg *config.LocatorCfg) (locator.Storage, error) {
	return locator.NewEtcdClient(cfg.EtcdAddresses)
}
