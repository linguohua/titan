package modules

import (
	"github.com/linguohua/titan/node/modules/dtypes"
	"github.com/linguohua/titan/region"
)

// NewRegion creates a new region instance using the given database path.
func NewRegion(dbPath dtypes.GeoDBPath) (region.Region, error) {
	return region.NewGeoLiteRegion(string(dbPath))
}
