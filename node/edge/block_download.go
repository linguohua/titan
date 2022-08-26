package edge

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/linguohua/titan/lib/token"
)

func (edge EdgeAPI) startDownloadServer(address string) {
	mux := http.NewServeMux()
	mux.HandleFunc(downloadSrvPath, edge.GetBlock)

	srv := &http.Server{
		Handler: mux,
		Addr:    address,
	}

	nl, err := net.Listen("tcp", address)
	if err != nil {
		log.Fatal(err)
	}

	log.Infof("startDownloadServer at %s", address)

	err = srv.Serve(nl)
	if err != nil {
		log.Fatal(err)
	}
}

func (edge EdgeAPI) GetBlock(w http.ResponseWriter, r *http.Request) {
	appName := r.Header.Get("App-Name")
	tk := r.Header.Get("Token")
	cidStr := r.URL.Query().Get("cid")

	log.Infof("GetBlock, appName:%s, token:%s,  cid:%s", appName, tk, cidStr)

	if !token.ValidToken(tk, edge.downloadSrvKey) {
		log.Errorf("Valid token %s error", tk)
		http.Error(w, fmt.Sprintf("Valid token %s error", tk), http.StatusBadRequest)
		return
	}

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
