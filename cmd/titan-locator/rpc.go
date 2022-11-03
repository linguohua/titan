package main

import (
	"context"
	"net/http"

	"github.com/linguohua/titan/lib/rpcenc"

	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/metrics/proxy"

	"github.com/filecoin-project/go-jsonrpc"
	"github.com/filecoin-project/go-jsonrpc/auth"

	"github.com/gorilla/mux"
	"github.com/linguohua/titan/node/handler"
)

// func blockDownload(a api.Edge) http.HandlerFunc {
// 	return func(w http.ResponseWriter, r *http.Request) {
// 		api := a.(edge.EdgeAPI)
// 		api.GetBlock(w, r)
// 	}
// }

func WorkerHandler(authv func(ctx context.Context, token string) ([]auth.Permission, error), a api.Location, permissioned bool) http.Handler {
	mux := mux.NewRouter()
	readerHandler, readerServerOpt := rpcenc.ReaderParamDecoder()
	rpcServer := jsonrpc.NewServer(readerServerOpt)

	wapi := proxy.MetricedLocationAPI(a)
	if permissioned {
		wapi = api.PermissionedLocationAPI(wapi)
	}

	rpcServer.Register("titan", wapi)

	mux.Handle("/rpc/v0", rpcServer)
	mux.Handle("/rpc/streams/v0/push/{uuid}", readerHandler)
	mux.PathPrefix("/").Handler(http.DefaultServeMux) // pprof

	if !permissioned {
		return mux
	}

	ah := &auth.Handler{
		Verify: authv,
		Next:   mux.ServeHTTP,
	}

	return handler.New(ah)
}
