package router

import (
	"net/http"

	"titan-ultra-network/log"

	"github.com/julienschmidt/httprouter"
	"github.com/sirupsen/logrus"
)

// StartEdgeServer 开启边缘节点的http服务
func StartEdgeServer(port string) {
	router := httprouter.New()

	// 测试
	router.GET("/test", test)

	logrus.Fatal(http.ListenAndServe(port, router))

	log.Infoln("port : ", port)
}
