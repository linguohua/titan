package region

import (
	"fmt"
	"net"

	"github.com/oschwald/geoip2-golang"
	"golang.org/x/xerrors"
)

// var reader *geoip2.Reader

// TypeGeoLite GeoLite
func TypeGeoLite() string {
	return "GeoLite"
}

func NewGeoLiteRegion(dbPath string) (Region, error) {
	gl := &geoLite{dbPath}

	db, err := geoip2.Open(gl.dbPath)
	if err != nil {
		return gl, err
	}
	defer db.Close()
	// reader = db

	return gl, nil
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

func (g geoLite) GetGeoInfo(ip string) (*GeoInfo, error) {
	geoInfo := DefaultGeoInfo(ip)
	if ip == "" {
		return geoInfo, xerrors.New("ip is nil")
	}

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
	country := record.Country.IsoCode
	city := unknown
	province := unknown
	// geoInfo.IsoCode = record.Country.IsoCode

	if record.City.Names["en"] != "" {
		city = record.City.Names["en"]
	}

	if len(record.Subdivisions) > 0 {
		province = record.Subdivisions[0].IsoCode
	}

	geoInfo.Latitude = record.Location.Latitude
	geoInfo.Longitude = record.Location.Longitude

	geoInfo.Geo = fmt.Sprintf("%s%s%s%s%s", country, separate, province, separate, city)

	return geoInfo, nil
}
