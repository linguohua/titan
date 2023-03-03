package locator

import (
	"fmt"
	"testing"
)

func TestDB(t *testing.T) {
	url := "user01:sql001@tcp(127.0.0.1:3306)/locator"
	db, err := NewSQLDB(url)
	if err != nil {
		t.Errorf("NewSQLDB error:%s", err.Error())
		return
	}
	defer db.close()

	cfg, err := db.getSchedulerCfg("https://192.168.0.138:67890/rpc/v0")
	if err != nil {
		fmt.Printf("err:%s", err.Error())
		return
	}
	fmt.Printf("cfg:%v", cfg)
}
