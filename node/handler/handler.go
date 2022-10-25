package handler

import (
	"context"
	"github.com/filecoin-project/go-jsonrpc/auth"
	logging "github.com/ipfs/go-log/v2"
	"net"
	"net/http"
)

var log = logging.Logger("handler")

type RequestIP struct{}

type Handler struct {
	*auth.Handler
}

func GetRequestIP(ctx context.Context) string {
	v, ok := ctx.Value(RequestIP{}).(string)
	if !ok {
		return ""
	}
	return v
}

func New(ah *auth.Handler) http.Handler {
	return &Handler{ah}
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	reqIP := r.Header.Get("X-Real-IP")
	if reqIP == "" {
		h, _, err := net.SplitHostPort(r.RemoteAddr)
		if err != nil {
			log.Errorf("could not get ip from: %s, err: %s", r.RemoteAddr, err)
		}
		reqIP = h
	}

	ctx := r.Context()
	ctx = context.WithValue(ctx, RequestIP{}, reqIP)

	h.Next(w, r.WithContext(ctx))
}
