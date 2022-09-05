package region

import (
	"fmt"
	"net"

	"github.com/oschwald/geoip2-golang"
)

// var reader *geoip2.Reader

// TypeGeoLite GeoLite
func TypeGeoLite() string {
	return "GeoLite"
}

// InitGeoLite init
func InitGeoLite(dbPath string) (Region, error) {
	gl := &geoLite{dbPath}

	db, err := geoip2.Open(gl.dbPath)
	if err != nil {
		return gl, err
	}
	defer db.Close()
	// reader = db

	return gl, nil
}

type geoLite struct {
	dbPath string
}

func (g geoLite) defaultGeoInfo(ip string) *GeoInfo {
	return &GeoInfo{
		City:      unknown,
		Country:   unknown,
		Province:  unknown,
		Latitude:  0,
		Longitude: 0,
		IP:        ip,
		Geo:       fmt.Sprintf("%s%s%s%s%s", unknown, separate, unknown, separate, unknown),
	}
}

func (g geoLite) GetGeoInfo(ip string) (*GeoInfo, error) {
	geoInfo := g.defaultGeoInfo(ip)

	db, err := geoip2.Open(g.dbPath)
	if err != nil {
		return geoInfo, err
	}
	defer db.Close()
	// If you are using strings that may be invalid, check that ip is not nil
	ipA := net.ParseIP(ip)
	record, err := db.City(ipA)
	if err != nil {
		return geoInfo, err
	}

	if record.Country.IsoCode == "" {
		return geoInfo, err
	}
	geoInfo.Country = record.Country.IsoCode
	// geoInfo.IsoCode = record.Country.IsoCode

	if record.City.Names["en"] != "" {
		geoInfo.City = record.City.Names["en"]
	}

	if len(record.Subdivisions) > 0 {
		geoInfo.Province = record.Subdivisions[0].IsoCode
	}

	geoInfo.Latitude = record.Location.Latitude
	geoInfo.Longitude = record.Location.Longitude

	geoInfo.Geo = fmt.Sprintf("%s%s%s%s%s", geoInfo.Country, separate, geoInfo.Province, separate, geoInfo.City)

	return geoInfo, nil
}
