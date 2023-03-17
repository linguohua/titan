package handler

import (
	"context"
	"net/http"

	"github.com/filecoin-project/go-jsonrpc/auth"
	logging "github.com/ipfs/go-log/v2"
)

var log = logging.Logger("handler")

type (
	RemoteAddr struct{}
	NodeID     struct{}
)

type Handler struct {
	handler *auth.Handler
}

func GetRemoteAddr(ctx context.Context) string {
	v, ok := ctx.Value(RemoteAddr{}).(string)
	if !ok {
		return ""
	}
	return v
}

func GetNodeID(ctx context.Context) string {
	v, ok := ctx.Value(NodeID{}).(string)
	if !ok {
		return ""
	}
	return v
}

func New(ah *auth.Handler) http.Handler {
	return &Handler{ah}
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	remoteAddr := r.Header.Get("X-Remote-Addr")
	if remoteAddr == "" {
		remoteAddr = r.RemoteAddr
	}

	nodeID := r.Header.Get("Node-ID")

	ctx := r.Context()
	ctx = context.WithValue(ctx, RemoteAddr{}, remoteAddr)
	ctx = context.WithValue(ctx, NodeID{}, nodeID)

	h.handler.ServeHTTP(w, r.WithContext(ctx))
}
