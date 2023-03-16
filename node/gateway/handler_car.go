package gateway

import (
	"context"
	"net/http"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-libipfs/blocks"
	"github.com/linguohua/titan/api/types"
)

func (gw *Gateway) serveCar(w http.ResponseWriter, r *http.Request, ticket *types.AccessTicket, carVersion string) {

}

type dagStore struct {
	gw *Gateway
}

func (ds dagStore) Get(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	return ds.gw.getBlock(ctx, c)
}
