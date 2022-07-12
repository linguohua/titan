package router

import (
	"encoding/json"
	"net/http"

	"titan-ultra-network/errorcode"
	"titan-ultra-network/log"

	"github.com/julienschmidt/httprouter"
	"github.com/sirupsen/logrus"
)

// GenericHTTPRsp 通用的http response回复
type GenericHTTPRsp struct {
	// 错误码，0表示成功
	ErrCode int `json:"errorCode"`
	// 字符串消息，用于客户端显示具体的错误信息等
	ErrMessage string `json:"errorMessage"`
	//
	Result interface{} `json:"result,omitempty"`
}

func replyGeneric(w http.ResponseWriter, errCode errorcode.ErrorCode, msg string, result interface{}) {
	gr := &GenericHTTPRsp{
		ErrCode:    int(errCode),
		ErrMessage: msg,
		Result:     result,
	}

	// log.Infof("replyGeneric ErrCode: %v,ErrMessage:%v,result:%v", errCode, msg, result)

	b, err := json.Marshal(gr)
	if err != nil {
		log.Errorf("replyGeneric failed: %v", err)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write(b)
}

// StartHTTPServer 开启钱包服务的http服务
func StartHTTPServer(port string) {
	router := httprouter.New()

	// 测试
	router.GET("/test", testSMS)

	logrus.Fatal(http.ListenAndServe(port, router))

	log.Infoln("port : ", port)
}
