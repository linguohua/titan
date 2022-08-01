package geoip

import (
	"fmt"
	"net"

	"github.com/oschwald/geoip2-golang"
)

// https://www.fecmall.com/topic/806

const unknown = "unknown"

// TypeGeoLite GeoLite
func TypeGeoLite() string {
	return "GeoLite"
}

// InitGeoLite init
func InitGeoLite(dbPath string) (GeoIP, error) {
	gl := &geoLite{dbPath}

	db, err := geoip2.Open(gl.dbPath)
	if err != nil {
		return gl, err
	}
	defer db.Close()

	return gl, nil
}

type geoLite struct {
	dbPath string
}

func (g geoLite) initGeoInfo(ip string) GeoInfo {
	return GeoInfo{
		City:      unknown,
		Country:   unknown,
		IsoCode:   unknown,
		Province:  unknown,
		Latitude:  0,
		Longitude: 0,
		IP:        ip,
		Geo:       unknown,
	}
}

func (g geoLite) GetGeoInfo(ip string) (GeoInfo, error) {
	geoInfo := g.initGeoInfo(ip)

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

	if record.Country.Names["en"] == "" {
		return geoInfo, err
	}

	geoInfo.City = record.City.Names["en"]
	geoInfo.Country = record.Country.Names["en"]
	geoInfo.IsoCode = record.Country.IsoCode
	geoInfo.Province = record.Subdivisions[0].Names["en"]
	geoInfo.Latitude = record.Location.Latitude
	geoInfo.Longitude = record.Location.Longitude

	geoInfo.Geo = fmt.Sprintf("%s-%s-%s", geoInfo.Country, geoInfo.Province, geoInfo.City)

	return geoInfo, nil
}
