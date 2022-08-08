package edge

import (
	"io"
	"net/http"
	"time"

	"golang.org/x/time/rate"
)

const (
	speedRate = 8000 << 10
	capacity  = 8000 << 10
)

var readerCount = 0

// or more verbosely you could call this a "limitedReadSeeker"
type lrs struct {
	io.ReadSeeker
	// This reader must not buffer but just do something simple
	// while passing through Read calls to the ReadSeeker
	r io.Reader
}

func (r lrs) Read(p []byte) (int, error) {
	return r.r.Read(p)
}

func changeLimiter(limiter *rate.Limiter) {
	log.Infof("auto change limiter, current limiter:%d", int(limiter.Limit()))
	for i := 0; i < 10; i++ {
		time.Sleep(10 * time.Second)

		limit := limiter.Limit()
		limit = limit / 2
		limiter.SetLimit(limit)
		limiter.SetBurst(int(limit))

		log.Infof("change limiter to %d", int(limit))
	}
}

func (edge EdgeAPI) GetBlock(w http.ResponseWriter, r *http.Request) {
	cid := r.URL.Query().Get("cid")
	log.Infof("GetBlock, cid:%s", cid)
	reader, err := edge.blockStore.GetReader(cid)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	defer reader.Close()

	now := time.Now().UnixMilli()

	lr := lrs{reader, NewReader(reader, edge.limiter)}
	http.ServeContent(w, r, cid, time.Now(), lr)

	costTime := time.Now().UnixMilli() - now
	size := reader.Size()

	var speedRate = int64(0)
	if costTime != 0 {
		speedRate = size / costTime * 1000
	}

	if edge.limiter.Limit() == 0 {
		edge.limiter.SetLimit(rate.Limit(speedRate))
		edge.limiter.SetBurst(int(speedRate))
		log.Infof("block_download set speed rate:%d", speedRate)
	}

	log.Infof("Download block %s costTime %d, size %d, speed %d", cid, costTime, size, speedRate)

	return
}
