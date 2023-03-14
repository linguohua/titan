package gateway

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/ipfs/go-datastore"
	"github.com/linguohua/titan/lib/limiter"
	"golang.org/x/time/rate"
)

func (gw *Gateway) blockHandler(w http.ResponseWriter, r *http.Request) {
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

	contentDisposition := fmt.Sprintf("attachment; filename=%s", c.String())
	w.Header().Set("Content-Disposition", contentDisposition)
	w.Header().Set("Content-Length", fmt.Sprintf("%d", len(blk.RawData())))

	now := time.Now()

	n, err := io.Copy(w, limiter.NewReader(bytes.NewReader(blk.RawData()), rate.NewLimiter(rate.Inf, 0)))
	if err != nil {
		log.Errorf("GetBlock, io.Copy error:%v", err)
		return
	}

	costTime := time.Since(now)
	speedRate := int64(0)
	if costTime != 0 {
		speedRate = int64(float64(n) / float64(costTime) * float64(time.Second))
	}

	log.Debugf("Download block %s costTime %d, size %d, speed %d", c.String(), costTime, n, speedRate)
}
