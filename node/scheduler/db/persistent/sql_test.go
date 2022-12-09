package persistent

import (
	"testing"
	"time"
)

const url = "root:123456@tcp(127.0.0.1:3306)/titan"

func TestSqlDB_InsertValidateResultInfo(t *testing.T) {
	db, err := InitSQL(url)
	if err != nil {
		t.Error(err.Error())
		return
	}
	info := new(ValidateResult)
	info.StartTime = time.Now()
	err = db.InsertValidateResultInfo(info)
	if err != nil {
		t.Error(err.Error())
		return
	}
}
