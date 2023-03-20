package region

import (
	"golang.org/x/xerrors"
)

const (
	unknown  = "unknown"
	separate = "-"
)

var defaultArea = ""

// Region geo interface
type Region interface {
	GetGeoInfo(ip string) (*GeoInfo, error)
}

// GeoInfo geo info
type GeoInfo struct {
	Latitude  float64
	Longitude float64
	IP        string
	Geo       string
}

var region Region

// NewRegion New Region
func NewRegion(dbPath, geoType, area string) error {
	var err error

	defaultArea = area

	switch geoType {
	case TypeGeoLite():
		region, err = InitGeoLite(dbPath)
	default:
		// panic("unknown Region type")
		err = xerrors.New("unknown Region type")
	}

	return err
}

// GetRegion Get Region
func GetRegion() Region {
	return region
}

// DefaultGeoInfo get default geo info
func DefaultGeoInfo(ip string) *GeoInfo {
	return &GeoInfo{
		Latitude:  0,
		Longitude: 0,
		IP:        ip,
		Geo:       defaultArea,
	}
}
