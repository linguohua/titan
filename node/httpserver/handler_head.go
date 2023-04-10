package httpserver

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/ipfs/go-cid"
)

func (hs *HttpServer) headHandler(w http.ResponseWriter, r *http.Request) {
	c, err := getCID(r.URL.Path)
	if err != nil {
		w.WriteHeader(http.StatusPreconditionFailed)
		return
	}
	// TODO get root from c
	if ok, err := hs.asset.HasBlock(context.Background(), c, c); err != nil || !ok {
		w.WriteHeader(http.StatusPreconditionFailed)
		return
	}

	w.WriteHeader(http.StatusOK)
}

// path=/ipfs/{cid}[/{path}]
func getCID(path string) (cid.Cid, error) {
	parts := strings.Split(path, "/")
	if len(parts) < 2 {
		return cid.Cid{}, fmt.Errorf("path not found")
	}

	cidStr := parts[1]
	c, err := cid.Decode(cidStr)
	if err != nil {
		return cid.Cid{}, err
	}

	return c, nil
}
