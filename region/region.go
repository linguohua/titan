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
	DefaultGeoInfo(ip string) *GeoInfo
}

// GeoInfo geo info
type GeoInfo struct {
	// City      string
	// Country   string
	// Province  string
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

	// if err != nil {
	// 	e := fmt.Sprintf("NewRegion err:%v , path:%v", err, dbPath)
	// 	panic(e)
	// }
	return err
}

// GetRegion Get Region
func GetRegion() Region {
	return region
}
