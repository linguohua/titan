package gateway

import (
	"net/http"

	"github.com/linguohua/titan/api/types"
)

func (gw *Gateway) serveCodec(w http.ResponseWriter, r *http.Request, credentials *types.Credentials) {
	http.Error(w, "not implement", http.StatusBadRequest)
}
