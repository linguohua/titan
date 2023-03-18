package gateway

import (
	"context"
	"fmt"
	"html"
	"net/http"
	"time"

	files "github.com/ipfs/go-ipfs-files"
	"github.com/ipfs/interface-go-ipfs-core/path"
	"github.com/linguohua/titan/api/types"
)

func (gw *Gateway) serveTAR(w http.ResponseWriter, r *http.Request, ticket *types.AccessTicket) {
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	contentPath := path.New(r.URL.Path)
	resolvedPath, err := gw.resolvePath(ctx, contentPath)
	if err != nil {
		http.Error(w, fmt.Sprintf("can not resolved path: %s", err.Error()), http.StatusBadRequest)
		return
	}

	// Get Unixfs file
	file, err := gw.getUnixFsNode(ctx, resolvedPath)
	if err != nil {
		err = fmt.Errorf("error getting UnixFS node for %s: %w", html.EscapeString(contentPath.String()), err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer file.Close()

	rootCID := resolvedPath.Cid()

	// Set Cache-Control and read optional Last-Modified time
	modtime := addCacheControlHeaders(w, r, contentPath, rootCID)

	// Weak Etag W/ because we can't guarantee byte-for-byte identical
	// responses, but still want to benefit from HTTP Caching. Two TAR
	// responses for the same CID will be logically equivalent,
	// but when TAR is streamed, then in theory, files and directories
	// may arrive in different order (depends on TAR lib and filesystem/inodes).
	etag := `W/` + getEtag(r, rootCID)
	w.Header().Set("Etag", etag)

	// Finish early if Etag match
	if r.Header.Get("If-None-Match") == etag {
		w.WriteHeader(http.StatusNotModified)
		return
	}

	// Set Content-Disposition
	var name string
	if urlFilename := r.URL.Query().Get("filename"); urlFilename != "" {
		name = urlFilename
	} else {
		name = rootCID.String() + ".tar"
	}
	setContentDispositionHeader(w, name, "attachment")

	// Construct the TAR writer
	tarw, err := files.NewTarWriter(w)
	if err != nil {
		http.Error(w, fmt.Sprintf("could not build tar writer: %s", err.Error()), http.StatusInternalServerError)
		return
	}
	defer tarw.Close()

	// Sets correct Last-Modified header. This code is borrowed from the standard
	// library (net/http/server.go) as we cannot use serveFile without throwing the entire
	// TAR into the memory first.
	if !(modtime.IsZero() || modtime.Equal(time.Unix(0, 0))) {
		w.Header().Set("Last-Modified", modtime.UTC().Format(http.TimeFormat))
	}

	w.Header().Set("Content-Type", "application/x-tar")
	w.Header().Set("X-Content-Type-Options", "nosniff") // no funny business in the browsers :^)

	// The TAR has a top-level directory (or file) named by the CID.
	if err := tarw.WriteFile(file, rootCID.String()); err != nil {
		log.Errorf("write tag file %s error: %s", rootCID.String(), err.Error())
		return
	}

	// TODO: limit rate and report to scheduler

}
