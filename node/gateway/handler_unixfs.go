package gateway

import (
	"context"
	"fmt"
	"net/http"

	files "github.com/ipfs/go-ipfs-files"
	"github.com/ipfs/interface-go-ipfs-core/path"
	"github.com/linguohua/titan/api/types"
)

func (gw *Gateway) serveUnixFS(w http.ResponseWriter, r *http.Request, ticket *types.Credentials) {
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	contentPath := path.New(r.URL.Path)
	resolvedPath, err := gw.resolvePath(ctx, contentPath)
	if err != nil {
		http.Error(w, fmt.Sprintf("can not resolved path: %s", err.Error()), http.StatusBadRequest)
		return
	}

	dr, err := gw.getUnixFsNode(ctx, resolvedPath)
	if err != nil {
		http.Error(w, fmt.Sprintf("error while getting UnixFS node: %s", err.Error()), http.StatusInternalServerError)
		return
	}
	defer dr.Close()

	// Handling Unixfs file
	if f, ok := dr.(files.File); ok {
		log.Debugw("serving unixfs file", "path", contentPath)
		gw.serveFile(w, r, ticket, f)
		return
	}

	// Handling Unixfs directory
	dir, ok := dr.(files.Directory)
	if !ok {
		http.Error(w, "unsupported UnixFS type", http.StatusInternalServerError)
		return
	}

	log.Debugw("serving unixfs directory", "path", contentPath)
	gw.serveDirectory(w, r, ticket, dir)
}

func (gw *Gateway) serveDirectory(w http.ResponseWriter, r *http.Request, ticket *types.Credentials, dir files.Directory) {
	http.Error(w, "dir list unsupport now", http.StatusBadRequest)
}
