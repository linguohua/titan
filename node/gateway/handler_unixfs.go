package gateway

import (
	"net/http"

	"github.com/linguohua/titan/api/types"
)

func (gw *Gateway) serveUnixFS(w http.ResponseWriter, r *http.Request, ticket *types.AccessTicket) {
}

func (gw *Gateway) fileHandler(w http.ResponseWriter, r *http.Request, ticket *types.AccessTicket) {
}

func (gw *Gateway) dirHandler(w http.ResponseWriter, r *http.Request, ticket *types.AccessTicket) {
	http.Error(w, "dir list unsupport now", http.StatusBadRequest)
}
