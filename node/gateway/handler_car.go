package gateway

import (
	"net/http"

	"github.com/linguohua/titan/api/types"
)

func (gw *Gateway) carHandler(w http.ResponseWriter, r *http.Request, ticket *types.AccessTicket, version string) {
}
