package gateway

import (
	"context"
	"crypto/rsa"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipfs/interface-go-ipfs-core/path"
	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/node/carfile/store"
	titanrsa "github.com/linguohua/titan/node/rsa"
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

type Gateway struct {
	cs                 *store.CarfileStore
	scheduler          api.Scheduler
	privateKey         *rsa.PrivateKey
	schedulerPublicKey *rsa.PublicKey
}

func NewGateway(cs *store.CarfileStore, scheduler api.Scheduler) *Gateway {
	privateKey, err := titanrsa.GeneratePrivateKey(1024)
	if err != nil {
		log.Panicf("generate private key error:%s", err.Error())
	}

	publicKey := titanrsa.PublicKey2Pem(&privateKey.PublicKey)
	if publicKey == nil {
		log.Panicf("can not convert public key to pem")
	}

	publicPem, err := scheduler.ExchangePublicKey(context.Background(), publicKey)
	if err != nil {
		log.Errorf("exchange public key from scheduler error %s", err.Error())
	}

	schedulerPublicKey, err := titanrsa.Pem2PublicKey(publicPem)
	if err != nil {
		log.Errorf("can not convert pem to public key %s", err.Error())
	}

	gw := &Gateway{cs: cs, scheduler: scheduler, privateKey: privateKey, schedulerPublicKey: schedulerPublicKey}

	return gw
}

func (gw *Gateway) AppendHandler(handler http.Handler) http.Handler {
	return &Handler{handler, gw}
}

func (gw *Gateway) handler(w http.ResponseWriter, r *http.Request) {
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
