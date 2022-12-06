package common

import (
	"regexp"

	"github.com/linguohua/titan/region"
)

var (
	// ServerArea Server Area
	ServerArea = "CN-GD-Shenzhen"
	whitelist  = []string{}
)

// InitServerArea set area
func InitServerArea(area string) {
	// log.Infof("server area :%s", area)
	ServerArea = area
}

func initAreaTable() {
}

func areaExist(area string) bool {
	if area == "" {
		return false
	}

	return area == ServerArea
}

// IPLegality ip
func IPLegality(ip string) (bool, *region.GeoInfo) {
	geoInfo, _ := region.GetRegion().GetGeoInfo(ip)

	if ipAddrACL(ip) {
		geoInfo.Geo = ServerArea
		return true, geoInfo
	}

	if areaExist(geoInfo.Geo) {
		return true, geoInfo
	}

	for _, p := range whitelist {
		if p == ip {
			geoInfo.Geo = ServerArea
			return true, geoInfo
		}
	}

	return false, geoInfo
}

func ipAddrACL(ip string) bool {
	if match0, _ := regexp.MatchString(`127\.0\.0\.1`, ip); match0 {
		return true
	}

	if match2, _ := regexp.MatchString(`10\.(25[0-5]|2[0-4][0-9]|[0-1]?[0-9]?[0-9])\.(25[0-5]|2[0-4][0-9]|[0-1]?[0-9]?[0-9])\.(25[0-5]|2[0-4][0-9]|[0-1]?[0-9]?[0-9])`, ip); match2 {
		if match3, _ := regexp.MatchString(`10\.10\.30\.1`, ip); match3 {
			return false
		}
		return true
	}

	if match4, _ := regexp.MatchString(`172\.((1[6-9])|(2[0-9])|(3[0-1]))\.(25[0-5]|2[0-4][0-9]|[0-1]?[0-9]?[0-9])\.(25[0-5]|2[0-4][0-9]|[0-1]?[0-9]?[0-9])`, ip); match4 {
		return true
	}

	if match5, _ := regexp.MatchString(
		`192\.168\.(25[0-5]|2[0-4][0-9]|[0-1]?[0-9]?[0-9])\.(25[0-5]|2[0-4][0-9]|[0-1]?[0-9]?[0-9])`, ip); match5 {
		return true
	}

	return false
}
