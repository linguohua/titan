package locator

import (
	"fmt"
	"testing"
)

func TestAddAccessPoint(t *testing.T) {
	url := "user01:sql001@tcp(127.0.0.1:3306)/location"
	db := newDB(url)
	defer db.close()
	// info, err := db.db.getDeviceInfo("12333333")
	// if err != nil {
	// 	fmt.Println("err:", err)
	// 	return
	// }
	// fmt.Printf("%v", info[0])
	// err := db.db.addCfg("sz", "http://localhost:1081", 10)
	// if err != nil {
	// 	fmt.Println("err:", err)
	// }
	count, err := db.db.countDeviceOnScheduler("http://localhost:1081")
	if err != nil {
		fmt.Println("err:", err)
		return
	}

	fmt.Printf("count:%d\n", count)
	// err := db.db.setDeviceInfo("1234567", "http://localhost:1081", "sz", false)
	// if err != nil {
	// 	fmt.Println("err:", err)
	// }
}
