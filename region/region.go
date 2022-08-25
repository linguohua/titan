package region

import (
	"fmt"
	"strings"
)

const (
	unknown  = "unknown"
	separate = "-"
)

// Region geo interface
type Region interface {
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

var region Region

// NewRegion New Region
func NewRegion(dbPath, geoType string) {
	var err error

	switch geoType {
	case TypeGeoLite():
		region, err = InitGeoLite(dbPath)
	default:
		panic("unknown Region type")
	}

	if err != nil {
		e := fmt.Sprintf("NewRegion err:%v , path:%v", err, dbPath)
		panic(e)
	}
}

// GetRegion Get Region
func GetRegion() Region {
	return region
}

// StringGeoToGeoInfo geo
func StringGeoToGeoInfo(geo string) *GeoInfo {
	geos := strings.Split(geo, separate)
	if len(geos) != 3 {
		return nil
	}

	return &GeoInfo{Country: geos[0], Province: geos[1], City: geos[2]}
}
