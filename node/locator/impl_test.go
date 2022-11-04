package locator

import (
	"fmt"
	"testing"

	"github.com/google/uuid"
)

func TestImpl(t *testing.T) {
	// gPath := "../../../city.mmdb"
	// err := region.NewRegion(gPath, region.TypeGeoLite())
	// if err != nil {
	// 	fmt.Println("NewRegion error:" + err.Error())
	// 	return
	// }

	// geoInfo, err := region.GetRegion().GetGeoInfo("192.168.0.1")
	// if err != nil {
	// 	fmt.Println("GetRegion error:" + err.Error())
	// 	return
	// }

	fmt.Printf("geoInfo:%v\n", uuid.New())
}
