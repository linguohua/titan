package edge

import (
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"
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
	cidStr := r.URL.Query().Get("cid")

	log.Infof("GetBlock, cid:%s", cidStr)

	// target, err := cid.Decode(cidStr)
	// if err != nil {
	// 	log.Errorf("GetBlock, decode cid error:%v", err)
	// 	http.Error(w, "Can not decode cid", http.StatusBadRequest)
	// 	return
	// }

	// // cid convert to vo
	// if target.Version() != 0 && target.Type() == cid.DagProtobuf {
	// 	target = cid.NewCidV0(target.Hash())
	// }

	// cidStr = fmt.Sprintf("%v", target)

	reader, err := edge.blockStore.GetReader(cidStr)
	if err != nil {
		log.Errorf("GetBlock, GetReader:%v", err)
		http.NotFound(w, r)
		return
	}
	defer reader.Close()

	contentDisposition := fmt.Sprintf("attachment; filename=%s", cidStr)
	w.Header().Set("Content-Disposition", contentDisposition)
	w.Header().Set("Content-Length", strconv.FormatInt(reader.Size(), 10))

	now := time.Now()

	n, err := io.Copy(w, NewReader(reader, edge.limiter))
	if err != nil {
		log.Errorf("GetBlock, io.Copy error:%v", err)
		return
	}

	costTime := time.Now().Sub(now)

	var speedRate = int64(0)
	if costTime != 0 {
		speedRate = int64(float64(n) / float64(costTime) * 1000000000)
	}

	log.Infof("Download block %s costTime %d, size %d, speed %d", cidStr, costTime, n, speedRate)

	return
}
