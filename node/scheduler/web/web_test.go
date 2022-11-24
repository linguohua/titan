package web

import (
	"fmt"
	"testing"

	"github.com/linguohua/titan/node/scheduler/db/persistent"
)

func TestWeb(t *testing.T) {
	url := "user01:sql001@tcp(127.0.0.1:3306)/test"
	err := persistent.NewDB(url, persistent.TypeSQL(), "server-111", "CN-GD-Shenzhen")
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	// for i := 0; i < 100; i++ {
	// 	roundID := fmt.Sprintf("000%d", i)
	// 	deviceID := fmt.Sprintf("00000%d", i)
	// 	validatorID := fmt.Sprintf("1111%d", i)
	// 	msg := "ok"
	// 	status := 1
	// 	startTime := "2022-11-24 15:00:00"
	// 	endTime := "2022-11-24 15:00:00"
	// 	info := &persistent.ValidateResult{RoundID: roundID, DeviceID: deviceID, ValidatorID: validatorID,
	// 		Msg: msg, Status: status, StartTime: startTime, EndTime: endTime, ServerName: "abc"}
	// 	err := persistent.GetDB().SetValidateResultInfo(info)
	// 	if err != nil {
	// 		log.Error(err)
	// 	}
	// }

	nodes, total, err := persistent.GetDB().GetValidateResults(0, 10)
	if err != nil {
		fmt.Println("GetNodeConnectionLogs error:", err.Error())
		return
	}

	fmt.Printf("total:%d, logs:%v", total, nodes)
}
