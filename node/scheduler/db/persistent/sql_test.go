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
	info.BlockNumber = 0
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
	info.BlockNumber = 9999
	info.Msg = "ok"
	info.EndTime = time.Now()
	info.Status = ValidateStatusSuccess.Int()
	info.Duration = 100000
	info.Bandwidth = 33.9
	err = db.UpdateSuccessValidateResultInfo(info)
	if err != nil {
		t.Error(err.Error())
		return
	}
}

func TestSqlDB_SummaryValidateMessage(t *testing.T) {
	db, err := InitSQL(url)
	if err != nil {
		t.Error(err.Error())
		return
	}
	start := time.Now().Add(-10 * time.Hour)
	end := time.Now()
	res, err := db.SummaryValidateMessage(start, end, 1, 10)
	if err != nil {
		t.Error(err.Error())
		return
	}
	t.Log(res)
}
