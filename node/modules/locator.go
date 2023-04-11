package modules

import (
	"github.com/linguohua/titan/node/locator"
	"github.com/linguohua/titan/node/modules/dtypes"
	"github.com/linguohua/titan/region"
	"go.uber.org/fx"
)

// AccessPointManagerParams Manager Params
type AccessPointManagerParams struct {
	fx.In

	Token dtypes.PermissionWriteToken
	UUID  dtypes.LocatorUUID
}

// NewAccessPointManager creates a new access point manager instance using the given parameters.
func NewAccessPointManager(params AccessPointManagerParams) *locator.AccessPointMgr {
	token := params.Token
	uuid := params.UUID
	return locator.NewAccessPointMgr(string(token), string(uuid))
}

// NewRegion creates a new region instance using the given database path.
func NewRegion(dbPath dtypes.GeoDBPath) (region.Region, error) {
	return region.NewGeoLiteRegion(string(dbPath))
}
