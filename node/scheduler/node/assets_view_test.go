package node

import (
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/linguohua/titan/node/scheduler/db"
	"github.com/linguohua/titan/node/sqldb"
)

func TestAssetsView(t *testing.T) {
	url := "user01:sql001@tcp(127.0.0.1:3306)/test"
	sqldb, err := sqldb.NewDB(url)
	if err != nil {
		t.Errorf("new db error:%s", err.Error())
		return
	}

	db := db.NewSQLDB(sqldb)
	mgr := NewManager(db, "1", nil, nil)

	c, err := cid.Decode("QmTcAg1KeDYJFpTJh3rkZGLhnnVKeXWNtjwPufjVvwPTpG")
	if err != nil {
		t.Errorf("decode error:%s", err.Error())
		return
	}

	if err := mgr.AddAsset("1111111", c); err != nil {
		t.Errorf("add asset error:%s", err.Error())
	}

	if err := mgr.RemoveAsset("1111111", c); err != nil {
		t.Errorf("remove asset error:%s", err.Error())
	}
}
