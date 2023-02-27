package locator

import (
	"fmt"
	"testing"
)

func TestDB(t *testing.T) {
	url := "user01:sql001@tcp(127.0.0.1:3306)/locator"
	db, err := newSQLDB(url)
	if err != nil {
		t.Errorf("newSQLDB error:%s", err.Error())
		return
	}
	defer db.close()

	cfgs, err := db.getCfgs("CN-GD-Shenzhen")
	if err != nil {
		fmt.Printf("err:%s", err.Error())
		return
	}
	fmt.Printf("cfgs:%v", cfgs)
}
