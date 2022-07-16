package router

import (
	"net/http"

	"titan-ultra-network/log"
	"titan-ultra-network/service/schedule"

	"github.com/julienschmidt/httprouter"
	"github.com/sirupsen/logrus"
)

// StartScheduleServer 开启调度中心的http服务
func StartScheduleServer(port string) {
	router := httprouter.New()
	// 测试
	router.GET("/test", test)

	router.GET("/ws/:deviceid", schedule.AcceptWebsocket)

	logrus.Fatal(http.ListenAndServe(port, router))

	log.Infoln("port : ", port)
}
