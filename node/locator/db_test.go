package locator

import (
	"testing"

	"github.com/linguohua/titan/node/scheduler/db"
)

func TestDB(t *testing.T) {
	t.Logf("TestDB")
	db, err := db.NewDB("user01:sql001@tcp(127.0.0.1:3306)/locator")
	if err != nil {
		t.Errorf("new db error:%s", err.Error())
		return
	}

	sqldb := NewSQLDB(db)

	nodeInfo, err := sqldb.getNodeInfo("e_a5a475d02480488e97ecc8e878c93caa")
	if err != nil {
		t.Errorf("get node info error:%s", err.Error())
		return
	}

	t.Logf("nodeInfo:%v", nodeInfo)
}
