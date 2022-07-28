package geoip

import (
	"net"

	"github.com/oschwald/geoip2-golang"
)

// https://www.fecmall.com/topic/806

// InitGeoLite init
func InitGeoLite(dbPath string) GeoIP {
	return &geoLite{dbPath}
}

type geoLite struct {
	dbPath string
}

func (g geoLite) getGeoInfo(addr string) (GeoInfo, error) {
	geoInfo := GeoInfo{}

	db, err := geoip2.Open(g.dbPath)
	if err != nil {
		return geoInfo, err
	}
	defer db.Close()
	// If you are using strings that may be invalid, check that ip is not nil
	ip := net.ParseIP(addr)
	record, err := db.City(ip)
	if err != nil {
		return geoInfo, err
	}
	// fmt.Printf("Portuguese (BR) city name: %v\n", record.City.Names["zh-CN"])
	// fmt.Printf("English subdivision name: %v\n", record.Subdivisions[0].Names["en"])
	// fmt.Printf("Russian country name: %v\n", record.Country.Names["en"])
	// fmt.Printf("ISO country code: %v\n", record.Country.IsoCode)
	// fmt.Printf("Coordinates: %v, %v\n", record.Location.Latitude, record.Location.Longitude)
	// fmt.Printf("English ---------------- name: %v\n", record.Subdivisions)
	geoInfo.City = record.City.Names["en"]
	geoInfo.Country = record.Country.Names["en"]
	geoInfo.IsoCode = record.Country.IsoCode
	geoInfo.Province = record.Subdivisions[0].Names["en"]
	geoInfo.Latitude = record.Location.Latitude
	geoInfo.Longitude = record.Location.Longitude

	return geoInfo, nil
}
