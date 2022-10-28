package scheduler

import (
	"github.com/linguohua/titan/region"
)

var (
	serverArea = "CN-GD-Shenzhen"
	whitelist  = []string{"192.168.0.26"}
)

// InitServerArea set area
func InitServerArea(area string) {
	log.Infof("server area :%s", area)
	serverArea = area
}

func initAreaTable() {
}

func areaExist(area string) bool {
	if area == "" {
		return false
	}

	return area == serverArea
}

func ipLegality(ip string) (bool, *region.GeoInfo) {
	geoInfo, _ := region.GetRegion().GetGeoInfo(ip)
	if areaExist(geoInfo.Geo) {
		return true, geoInfo
	}

	for _, p := range whitelist {
		if p == ip {
			geoInfo.Geo = serverArea
			return true, geoInfo
		}
	}

	return false, geoInfo
}
