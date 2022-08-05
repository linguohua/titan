package geoip

import "fmt"

// GeoIP geo interface
type GeoIP interface {
	GetGeoInfo(ip string) (GeoInfo, error)
}

// GeoInfo geo info
type GeoInfo struct {
	City      string
	Country   string
	Province  string
	Latitude  float64
	Longitude float64
	// IsoCode   string
	IP  string
	Geo string
}

var geoIP GeoIP

// NewGeoIP New GeoIP
func NewGeoIP(dbPath, geoType string) {
	var err error

	switch geoType {
	case TypeGeoLite():
		geoIP, err = InitGeoLite(dbPath)
	default:
		panic("unknown GeoIP type")
	}

	if err != nil {
		e := fmt.Sprintf("NewGeoIP err:%v , path:%v", err, dbPath)
		panic(e)
	}
}

// GetGeoIP Get GeoIP
func GetGeoIP() GeoIP {
	return geoIP
}
