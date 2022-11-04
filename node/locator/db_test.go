package locator

import (
	"fmt"
	"testing"
)

func TestDB(t *testing.T) {
	url := "user01:sql001@tcp(127.0.0.1:3306)/location"
	db := newDB(url)
	defer db.close()

	err := db.addAccessPoints("sz", "http://192.168.0.1:1080/rpc/v0", 100, "111111111111111")
	if err != nil {
		fmt.Printf("err:%s", err.Error())
		return
	}
	// db.addAccessPoints("gz", "http://localhost:1080/rpc/v0", 100)
	// db.removeAccessPoints("sz")
	// areaIDs, err := db.listAccessPoints()
	// if err != nil {
	// 	fmt.Printf("err:%s", err.Error())
	// 	return
	// }
	// fmt.Printf("areaID %v", areaIDs)

}
