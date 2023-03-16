package gateway

import (
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/linguohua/titan/api/types"
	"github.com/linguohua/titan/lib/limiter"
	"golang.org/x/time/rate"
)

var (
	onlyAscii = regexp.MustCompile("[[:^ascii:]]")
)

func (gw *Gateway) blockHandler(w http.ResponseWriter, r *http.Request, ticket *types.AccessTicket) {
	c, err := getCID(r.URL.Path)
	if err != nil {
		http.Error(w, fmt.Sprintf("can not get cid from path: %s", err.Error()), http.StatusBadRequest)
		return
	}

	log.Debugf("blockHandler, block cid %s", c.String())

	blk, err := gw.cs.Block(c)
	if err != nil {
		if err == datastore.ErrNotFound {
			http.NotFound(w, r)
			return
		}

		http.Error(w, fmt.Sprintf("colud not get block: %s", err.Error()), http.StatusBadRequest)
		return
	}

	filename := getFilename(r, c)
	setHeaderForBlockHandler(w, r, filename)

	now := time.Now()

	reader := limiter.ReaderFromBytes(blk.RawData(), rate.NewLimiter(rate.Inf, 0))

	http.ServeContent(w, r, filename, time.Now(), reader)

	costTime := time.Since(now)
	speedRate := int64(0)
	if costTime != 0 {
		speedRate = int64(float64(len(blk.RawData())) / float64(costTime) * float64(time.Second))
	}

	log.Debugf("Download block %s costTime %d, size %d, speed %d", c.String(), costTime, len(blk.RawData()), speedRate)
}

func getFilename(r *http.Request, c cid.Cid) string {
	filename := r.URL.Query().Get("filename")
	if filename == "" {
		filename = c.String() + ".bin"
	}

	return filename
}

func setHeaderForBlockHandler(w http.ResponseWriter, r *http.Request, filename string) {
	utf8Name := url.PathEscape(filename)
	asciiName := url.PathEscape(onlyAscii.ReplaceAllLiteralString(filename, "_"))
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=\"%s\"; filename*=UTF-8''%s", asciiName, utf8Name))
	w.Header().Set("Content-Type", "application/vnd.ipld.raw")
}
