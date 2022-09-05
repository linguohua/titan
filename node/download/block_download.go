package download

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	logging "github.com/ipfs/go-log/v2"
	"github.com/linguohua/titan/lib/token"
	"github.com/linguohua/titan/node/helper"
	"github.com/linguohua/titan/stores"
	"golang.org/x/time/rate"
)

var log = logging.Logger("edge")

type BlockDownload struct {
	limiter        *rate.Limiter
	downloadSrvURL string
	blockStore     stores.BlockStore
	downloadSrvKey string
}

func NewBlockDownload(limiter *rate.Limiter, blockStore stores.BlockStore, downloadSrvKey, downloadSrvAddr, internalIP string) *BlockDownload {
	var downloadSrvURL = parseDownloadSrvURL(downloadSrvAddr, internalIP)
	var blockDownload = &BlockDownload{limiter: limiter, blockStore: blockStore, downloadSrvKey: downloadSrvKey, downloadSrvURL: downloadSrvURL}
	go blockDownload.startDownloadServer(downloadSrvAddr)

	return blockDownload
}

func parseDownloadSrvURL(downloadSrvAddr, internalIP string) string {
	const unspecifiedAddress = "0.0.0.0"
	addressSlice := strings.Split(downloadSrvAddr, ":")
	if len(addressSlice) != 2 {
		log.Fatal("Invalid downloadSrvAddr")
	}

	if addressSlice[0] == unspecifiedAddress {
		return fmt.Sprintf("http://%s:%s%s", internalIP, addressSlice[1], helper.DownloadSrvPath)
	}

	return fmt.Sprintf("http://%s%s", downloadSrvAddr, helper.DownloadSrvPath)
}

func (bd *BlockDownload) getBlock(w http.ResponseWriter, r *http.Request) {
	appName := r.Header.Get("App-Name")
	tk := r.Header.Get("Token")
	cidStr := r.URL.Query().Get("cid")

	log.Infof("GetBlock, App-Name:%s, Token:%s,  cid:%s", appName, tk, cidStr)

	if !token.ValidToken(tk, bd.downloadSrvKey) {
		log.Errorf("Valid token %s error", tk)
		http.Error(w, fmt.Sprintf("Valid token %s error", tk), http.StatusBadRequest)
		return
	}

	reader, err := bd.blockStore.GetReader(cidStr)
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

	n, err := io.Copy(w, NewReader(reader, bd.limiter))
	if err != nil {
		log.Errorf("GetBlock, io.Copy error:%v", err)
		return
	}

	costTime := time.Now().Sub(now)

	var speedRate = int64(0)
	if costTime != 0 {
		speedRate = int64(float64(n) / float64(costTime) * float64(time.Second))
	}

	log.Infof("Download block %s costTime %d, size %d, speed %d", cidStr, costTime, n, speedRate)

	return
}

func (bd *BlockDownload) startDownloadServer(address string) {
	mux := http.NewServeMux()
	mux.HandleFunc(helper.DownloadSrvPath, bd.getBlock)

	srv := &http.Server{
		Handler: mux,
		Addr:    address,
	}

	nl, err := net.Listen("tcp", address)
	if err != nil {
		log.Fatal(err)
	}

	log.Infof("download server listen on %s", address)

	err = srv.Serve(nl)
	if err != nil {
		log.Fatal(err)
	}
}

func (bd *BlockDownload) SetDownloadSpeed(speedRate int64) error {
	log.Infof("set download speed %d", speedRate)
	if bd.limiter == nil {
		return fmt.Errorf("edge.limiter == nil")
	}
	bd.limiter.SetLimit(rate.Limit(speedRate))
	bd.limiter.SetBurst(int(speedRate))

	return nil
}

func (bd *BlockDownload) UnlimitDownloadSpeed() error {
	log.Infof("UnlimitDownloadSpeed")
	if bd.limiter == nil {
		return fmt.Errorf("edge.limiter == nil")
	}

	bd.limiter.SetLimit(rate.Inf)
	bd.limiter.SetBurst(0)

	return nil
}

func (bd *BlockDownload) GenerateDownloadToken() (string, error) {
	log.Debug("GenerateDownloadToken")
	return token.GenerateToken(bd.downloadSrvKey, time.Now().Add(30*24*time.Hour).Unix())
}

func (bd *BlockDownload) GetDownloadSrvURL() string {
	log.Debug("GenerateDownloadToken")
	return bd.downloadSrvURL
}

func (bd *BlockDownload) GetRateLimit() int64 {
	log.Debug("GenerateDownloadToken")
	return int64(bd.limiter.Limit())
}
