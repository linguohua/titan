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
	info.DeviceID = "001"
	info.ValidatorID = "1"
	info.RoundID = 100
	info.ServerName = "test"
	info.Status = ValidateStatusCreate.Int()
	info.StartTime = time.Now()
	err = db.InsertValidateResultInfo(info)
	if err != nil {
		t.Error(err.Error())
		return
	}
}

func TestSqlDB_UpdateValidateResultInfo(t *testing.T) {
	db, err := InitSQL(url)
	if err != nil {
		t.Error(err.Error())
		return
	}
	info := new(ValidateResult)
	info.DeviceID = "001"
	info.RoundID = 100
	info.Msg = "ok"
	info.EndTime = time.Now()
	info.Status = ValidateStatusSuccess.Int()
	err = db.UpdateValidateResultInfo(info)
	if err != nil {
		t.Error(err.Error())
		return
	}
}
