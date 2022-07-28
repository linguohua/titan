package geoip

// GeoIP geo interface
type GeoIP interface {
	getGeoInfo(addr string) (GeoInfo, error)
}

// GeoInfo geo info
type GeoInfo struct {
	City      string
	Country   string
	Province  string
	Latitude  float64
	Longitude float64
	IsoCode   string
}

var geoIP GeoIP

// NewGeoIP New GeoIP
func NewGeoIP(dbPath string) {
	geoIP = InitGeoLite(dbPath)
}

// GetGeoIP Get GeoIP
func GetGeoIP() GeoIP {
	return geoIP
}
