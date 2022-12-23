package area

import (
	"net"

	"github.com/linguohua/titan/region"
)

var (
	serverArea string
)

// InitServerArea set area
func InitServerArea(area string) {
	// log.Infof("server area :%s", area)
	serverArea = area
}

// IsExist area exist
func IsExist(area string) bool {
	if area == "" {
		return false
	}

	return area == serverArea
}

// GetServerArea get area
func GetServerArea() string {
	return serverArea
}

// GetGeoInfoWithIP get geo info
func GetGeoInfoWithIP(ip string) (*region.GeoInfo, bool) {
	geoInfo, _ := region.GetRegion().GetGeoInfo(ip)

	if hasLocalIP(ip) {
		geoInfo.Geo = serverArea
		return geoInfo, true
	}

	if IsExist(geoInfo.Geo) {
		return geoInfo, true
	}

	return nil, false
}

func hasLocalIP(ipStr string) bool {
	ip := net.ParseIP(ipStr)

	if ip.IsLoopback() {
		return true
	}

	ip4 := ip.To4()
	if ip4 == nil {
		return false
	}

	return ip4[0] == 10 || // 10.0.0.0/8
		(ip4[0] == 172 && ip4[1] >= 16 && ip4[1] <= 31) || // 172.16.0.0/12
		(ip4[0] == 169 && ip4[1] == 254) || // 169.254.0.0/16
		(ip4[0] == 192 && ip4[1] == 168) // 192.168.0.0/16
}
