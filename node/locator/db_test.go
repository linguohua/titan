package locator

import (
	"fmt"
	"testing"
)

func TestDB(t *testing.T) {
	url := "user01:sql001@tcp(127.0.0.1:3306)/locator"
	db := newDB(url)
	defer db.close()

	cfgs, err := db.db.getCfgs("CN-GD-Shenzhen")
	if err != nil {
		fmt.Printf("err:%s", err.Error())
		return
	}
	fmt.Printf("cfgs:%v", cfgs)
	// info, err := db.getDeviceInfo("525e7729506711ed8c2c902e1671f843")
	// if err != nil {
	// 	fmt.Printf("err:%s", err.Error())
	// 	return
	// }
	// 525e7729506711ed8c2c902e1671f843	http://192.168.0.26:3456/rpc/v0	CN-GD-Shenzhen	1
	// db.db.setDeviceInfo("525e7729506711ed8c2c902e1671f843", "http://192.168.0.29:3456/rpc/v0", "CN-GD-Shenzhenabc", false)
	// l := &Locator{db: db}
	// l.DeviceOffline(nil, info.DeviceID)
	// db.addAccessPoints("gz", "http://localhost:1080/rpc/v0", 100)
	// db.removeAccessPoints("sz")
	// areaIDs, err := db.listAccessPoints()
	// if err != nil {
	// 	fmt.Printf("err:%s", err.Error())
	// 	return
	// }
	// fmt.Printf("info %v", *info)

}
