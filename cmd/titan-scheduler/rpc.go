package main

import (
	"net/http"

	"titan-ultra-network/lib/rpcenc"

	"titan-ultra-network/api"
	"titan-ultra-network/metrics/proxy"

	"github.com/filecoin-project/go-jsonrpc"
	"github.com/filecoin-project/go-jsonrpc/auth"
	"github.com/gorilla/mux"
)

func WorkerHandler(a api.Scheduler, permissioned bool) http.Handler {
	mux := mux.NewRouter()
	readerHandler, readerServerOpt := rpcenc.ReaderParamDecoder()
	rpcServer := jsonrpc.NewServer(readerServerOpt)

	wapi := proxy.MetricedSchedulerAPI(a)
	if permissioned {
		wapi = api.PermissionedSchedulerAPI(wapi)
	}

	rpcServer.Register("Filecoin", wapi)

	mux.Handle("/rpc/v0", rpcServer)
	mux.Handle("/rpc/streams/v0/push/{uuid}", readerHandler)
	mux.PathPrefix("/").Handler(http.DefaultServeMux) // pprof

	if !permissioned {
		return mux
	}

	ah := &auth.Handler{
		// Verify: authv,
		Next: mux.ServeHTTP,
	}
	return ah
}
