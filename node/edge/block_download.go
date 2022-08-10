package edge

import (
	"fmt"
	"io"
	"net/http"
	"time"

	"golang.org/x/time/rate"
)

// const (
// 	speedRate = 8000 << 10
// 	capacity  = 8000 << 10
// )

// var readerCount = 0

// or more verbosely you could call this a "limitedReadSeeker"
// type lrs struct {
// 	io.ReadSeeker
// 	// This reader must not buffer but just do something simple
// 	// while passing through Read calls to the ReadSeeker
// 	r io.Reader
// }

// func (r lrs) Read(p []byte) (int, error) {
// 	return r.r.Read(p)
// }

func (edge EdgeAPI) GetBlock(w http.ResponseWriter, r *http.Request) {
	cid := r.URL.Query().Get("cid")
	log.Infof("GetBlock, cid:%s", cid)
	reader, err := edge.blockStore.GetReader(cid)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	defer reader.Close()

	contentDisposition := fmt.Sprintf("attachment; filename=%s", cid)
	w.Header().Set("Content-Disposition", contentDisposition)

	now := time.Now()

	n, err := io.Copy(w, reader)
	if err != nil {
		log.Errorf("GetBlock, io.Copy error:%v", err)
		return
	}

	costTime := time.Now().Sub(now)

	var speedRate = int64(0)
	if costTime != 0 {
		speedRate = int64(float64(n) / float64(costTime) * 1000000000)
	}

	if edge.limiter.Limit() == rate.Inf {
		edge.limiter.SetLimit(rate.Limit(speedRate))
		edge.limiter.SetBurst(int(speedRate))
		log.Infof("block_download set speed rate:%d", speedRate)
	}

	log.Infof("Download block %s costTime %d, size %d, speed %d", cid, costTime, n, speedRate)

	return
}
