package gateway

import (
	"bytes"
	"context"
	"fmt"
	"net/http"

	"github.com/ipfs/interface-go-ipfs-core/path"
	"github.com/linguohua/titan/api/types"
)

func (gw *Gateway) serveRawBlock(w http.ResponseWriter, r *http.Request, credentials *types.Credentials) {
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	contentPath := path.New(r.URL.Path)
	resolvedPath, err := gw.resolvePath(ctx, contentPath)
	if err != nil {
		http.Error(w, fmt.Sprintf("can not resolved path: %s", err.Error()), http.StatusBadRequest)
		return
	}

	c := resolvedPath.Cid()
	block, err := gw.storage.GetBlock(ctx, c)
	if err != nil {
		http.Error(w, fmt.Sprintf("can not get block %s, %s", c.String(), err.Error()), http.StatusInternalServerError)
		return
	}

	content := bytes.NewReader(block.RawData())

	// Set Content-Disposition
	var name string
	if urlFilename := r.URL.Query().Get("filename"); urlFilename != "" {
		name = urlFilename
	} else {
		name = c.String() + ".bin"
	}
	setContentDispositionHeader(w, name, "attachment")

	// Set remaining headers
	w.Header().Set("Content-Type", "application/vnd.ipld.raw")
	w.Header().Set("X-Content-Type-Options", "nosniff") // no funny business in the browsers :^)

	modtime := addCacheControlHeaders(w, r, contentPath, c)
	// ServeContent will take care of
	// If-None-Match+Etag, Content-Length and range requests
	http.ServeContent(w, r, name, modtime, content)

	// TODO: limit rate and report to scheduler
}
