package modules

import (
	"github.com/linguohua/titan/node/locator"
	"github.com/linguohua/titan/node/modules/dtypes"
	"github.com/linguohua/titan/region"
	"go.uber.org/fx"
)

type AccessPointManagerParams struct {
	fx.In

	Token dtypes.PermissionWriteToken
	UUID  dtypes.LocatorUUID
}

func NewAccessPointManager(params AccessPointManagerParams) *locator.AccessPointMgr {
	token := params.Token
	uuid := params.UUID
	return locator.NewAccessPointMgr(string(token), string(uuid))
}

func NewRegion(dbPath dtypes.GeoDBPath) (region.Region, error) {
	return region.NewGeoLiteRegion(string(dbPath))
}
