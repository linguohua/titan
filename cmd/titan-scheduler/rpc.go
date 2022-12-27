package main

import (
	"net/http"

	"github.com/linguohua/titan/lib/rpcenc"
	"github.com/linguohua/titan/node/handler"

	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/metrics/proxy"

	"github.com/filecoin-project/go-jsonrpc"
	"github.com/filecoin-project/go-jsonrpc/auth"
	"github.com/gorilla/mux"

	_ "net/http/pprof"
)

func schedulerHandler(a api.Scheduler, permissioned bool) http.Handler {
	mux := mux.NewRouter()
	readerHandler, readerServerOpt := rpcenc.ReaderParamDecoder()
	rpcServer := jsonrpc.NewServer(readerServerOpt)

	wapi := proxy.MetricedSchedulerAPI(a)
	if permissioned {
		wapi = api.PermissionedSchedulerAPI(wapi)
	}

	rpcServer.Register("titan", wapi)

	mux.Handle("/rpc/v0", rpcServer)
	mux.Handle("/rpc/streams/v0/push/{uuid}", readerHandler)
	mux.PathPrefix("/").Handler(http.DefaultServeMux) // pprof

	if !permissioned {
		return mux
	}

	ah := &auth.Handler{
		Verify: a.AuthNodeVerify,
		Next:   mux.ServeHTTP,
	}

	return handler.New(ah)
}
