package gateway

import (
	"fmt"
	"net/http"
	"strings"

	logging "github.com/ipfs/go-log/v2"
	"github.com/linguohua/titan/node/carfile/store"
)

var log = logging.Logger("gateway")

const (
	path = "/ipfs"
)

type Handler struct {
	handler http.Handler
	gw      *Gateway
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case strings.HasPrefix(r.URL.Path, path):
		h.gw.handler(w, r)
	default:
		h.handler.ServeHTTP(w, r)
	}

}

type Gateway struct {
	cs *store.CarfileStore
}

func NewGateway() *Gateway {
	return &Gateway{}
}

func (gw *Gateway) AppendHandler(handler http.Handler) http.Handler {
	return &Handler{handler, gw}
}

func (gw *Gateway) handler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodHead:
		gw.headHandler(w, r)
	case http.MethodGet:
		gw.getHandler(w, r)
	default:
		http.Error(w, fmt.Sprintf("method %s not allowed", r.Method), http.StatusBadRequest)
	}
}
