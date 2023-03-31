package gateway

import (
	"context"
	"fmt"
	"net/http"

	"github.com/ipfs/interface-go-ipfs-core/path"
	"github.com/linguohua/titan/api/types"
)

func (gw *Gateway) serveCar(w http.ResponseWriter, r *http.Request, credentials *types.Credentials, carVersion string) {
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	switch carVersion {
	case "": // noop, client does not care about version
	case "1":
	case "2":
	default:
		http.Error(w, fmt.Sprintf("not support car version %s", carVersion), http.StatusBadRequest)
		return
	}

	contentPath := path.New(r.URL.Path)
	resolvedPath, err := gw.resolvePath(ctx, contentPath)
	if err != nil {
		http.Error(w, fmt.Sprintf("can not resolved path: %s", err.Error()), http.StatusBadRequest)
		return
	}
	rootCID := resolvedPath.Cid()

	has, err := gw.storage.HasCar(rootCID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if !has {
		http.Error(w, fmt.Sprintf("can not found car %s", contentPath.String()), http.StatusNotFound)
		return
	}

	// Set Content-Disposition
	var name string
	if urlFilename := r.URL.Query().Get("filename"); urlFilename != "" {
		name = urlFilename
	} else {
		name = rootCID.String() + ".car"
	}
	setContentDispositionHeader(w, name, "attachment")

	// Set Cache-Control (same logic as for a regular files)
	addCacheControlHeaders(w, r, contentPath, rootCID)

	// Weak Etag W/ because we can't guarantee byte-for-byte identical
	// responses, but still want to benefit from HTTP Caching. Two CAR
	// responses for the same CID and selector will be logically equivalent,
	// but when CAR is streamed, then in theory, blocks may arrive from
	// datastore in non-deterministic order.
	etag := `W/` + getEtag(r, rootCID)
	w.Header().Set("Etag", etag)

	// Finish early if Etag match
	if r.Header.Get("If-None-Match") == etag {
		w.WriteHeader(http.StatusNotModified)
		return
	}

	w.Header().Set("Content-Type", "application/vnd.ipld.car; version=1")
	w.Header().Set("X-Content-Type-Options", "nosniff") // no funny business in the browsers :^)

	modtime := addCacheControlHeaders(w, r, contentPath, rootCID)

	reader, err := gw.storage.GetCar(rootCID)
	if err != nil {
		http.Error(w, fmt.Sprintf("not support car version %s", carVersion), http.StatusInternalServerError)
		return
	}
	defer reader.Close() //nolint:errcheck  // ignore error

	// If-None-Match+Etag, Content-Length and range requests
	http.ServeContent(w, r, name, modtime, reader)

	// TODO: limit rate and report to scheduler
}
