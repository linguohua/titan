package gateway

import (
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipfs/interface-go-ipfs-core/path"
)

var log = logging.Logger("gateway")

const (
	ipfsPathPrefix        = "/ipfs"
	immutableCacheControl = "public, max-age=29030400, immutable"
)

var (
	onlyAscii = regexp.MustCompile("[[:^ascii:]]")
)

type Handler struct {
	handler http.Handler
	gw      *Gateway
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case strings.HasPrefix(r.URL.Path, ipfsPathPrefix):
		h.gw.handler(w, r)
	default:
		h.handler.ServeHTTP(w, r)
	}

}

func (gw *Gateway) NewHandler(handler http.Handler) http.Handler {
	return &Handler{handler, gw}
}

func (gw *Gateway) handler(w http.ResponseWriter, r *http.Request) {
	log.Debugf("handler path: %s", r.URL.Path)

	switch r.Method {
	case http.MethodHead:
		gw.headHandler(w, r)
	case http.MethodGet:
		gw.getHandler(w, r)
	default:
		http.Error(w, fmt.Sprintf("method %s not allowed", r.Method), http.StatusBadRequest)
	}
}

// generate Etag value based on HTTP request and CID
func getEtag(r *http.Request, cid cid.Cid) string {
	prefix := `"`
	suffix := `"`
	responseFormat, _, err := customResponseFormat(r)
	if err == nil && responseFormat != "" {
		// application/vnd.ipld.foo → foo
		// application/x-bar → x-bar
		shortFormat := responseFormat[strings.LastIndexAny(responseFormat, "/.")+1:]
		// Etag: "cid.shortFmt" (gives us nice compression together with Content-Disposition in block (raw) and car responses)
		suffix = `.` + shortFormat + suffix
	}
	// TODO: include selector suffix when https://github.com/ipfs/kubo/issues/8769 lands
	return prefix + cid.String() + suffix
}

// Set Content-Disposition to arbitrary filename and disposition
func setContentDispositionHeader(w http.ResponseWriter, filename string, disposition string) {
	utf8Name := url.PathEscape(filename)
	asciiName := url.PathEscape(onlyAscii.ReplaceAllLiteralString(filename, "_"))
	w.Header().Set("Content-Disposition", fmt.Sprintf("%s; filename=\"%s\"; filename*=UTF-8''%s", disposition, asciiName, utf8Name))
}

func addCacheControlHeaders(w http.ResponseWriter, r *http.Request, contentPath path.Path, fileCid cid.Cid) (modtime time.Time) {
	// Set Etag to based on CID (override whatever was set before)
	w.Header().Set("Etag", getEtag(r, fileCid))

	// Set Cache-Control and Last-Modified based on contentPath properties
	if contentPath.Mutable() {
		// mutable namespaces such as /ipns/ can't be cached forever

		/* For now we set Last-Modified to Now() to leverage caching heuristics built into modern browsers:
		 * https://github.com/ipfs/kubo/pull/8074#pullrequestreview-645196768
		 * but we should not set it to fake values and use Cache-Control based on TTL instead */
		modtime = time.Now()

		// TODO: set Cache-Control based on TTL of IPNS/DNSLink: https://github.com/ipfs/kubo/issues/1818#issuecomment-1015849462
		// TODO: set Last-Modified based on /ipns/ publishing timestamp?
	} else {
		// immutable! CACHE ALL THE THINGS, FOREVER! wolololol
		w.Header().Set("Cache-Control", immutableCacheControl)

		// Set modtime to 'zero time' to disable Last-Modified header (superseded by Cache-Control)
		modtime = time.Unix(0, 0)

		// TODO: set Last-Modified? - TBD - /ipfs/ modification metadata is present in unixfs 1.5 https://github.com/ipfs/kubo/issues/6920?
	}

	return modtime
}

func getFilename(contentPath path.Path) string {
	s := contentPath.String()
	if strings.HasPrefix(s, ipfsPathPrefix) {
		// Don't want to treat ipfs.io in /ipns/ipfs.io as a filename.
		return ""
	}
	return s
}

// Set Content-Disposition if filename URL query param is present, return preferred filename
func addContentDispositionHeader(w http.ResponseWriter, r *http.Request, contentPath path.Path) string {
	/* This logic enables:
	 * - creation of HTML links that trigger "Save As.." dialog instead of being rendered by the browser
	 * - overriding the filename used when saving subresource assets on HTML page
	 * - providing a default filename for HTTP clients when downloading direct /ipfs/CID without any subpath
	 */

	// URL param ?filename=cat.jpg triggers Content-Disposition: [..] filename
	// which impacts default name used in "Save As.." dialog
	name := getFilename(contentPath)
	urlFilename := r.URL.Query().Get("filename")
	if urlFilename != "" {
		disposition := "inline"
		// URL param ?download=true triggers Content-Disposition: [..] attachment
		// which skips rendering and forces "Save As.." dialog in browsers
		if r.URL.Query().Get("download") == "true" {
			disposition = "attachment"
		}
		setContentDispositionHeader(w, urlFilename, disposition)
		name = urlFilename
	}
	return name
}
