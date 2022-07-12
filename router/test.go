package router

import (
	"fmt"
	"net/http"

	"titan-ultra-network/errorcode"

	"github.com/julienschmidt/httprouter"
)

// 1EiB=1024PiB=1024^2TiB

func testSMS(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	query := r.URL.Query()
	msg := query.Get("msg")

	replyGeneric(w, errorcode.Success, fmt.Sprintf("%s", msg), nil)
}
