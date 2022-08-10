package geoip

import (
	"fmt"
	"strings"
)

const (
	unknown  = "unknown"
	separate = "-"
)

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
	IP        string
	Geo       string
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

// StringGeoToGeoInfo geo
func StringGeoToGeoInfo(geo string) *GeoInfo {
	geos := strings.Split(geo, separate)
	if len(geos) < 3 {
		return nil
	}

	return &GeoInfo{Country: geos[0], Province: geos[1], City: geos[2]}
}
