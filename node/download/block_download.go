package download

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	logging "github.com/ipfs/go-log/v2"
	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/blockstore"
	"github.com/linguohua/titan/lib/token"
	"github.com/linguohua/titan/node/helper"
	"golang.org/x/time/rate"
)

var log = logging.Logger("download")

type BlockDownload struct {
	limiter        *rate.Limiter
	downloadSrvURL string
	blockStore     blockstore.BlockStore
	downloadSrvKey string
	scheduler      api.Scheduler
	deviceID       string
}

func NewBlockDownload(limiter *rate.Limiter, params *helper.NodeParams, externalIP, deviceID string) *BlockDownload {
	var downloadSrvURL = parseDownloadSrvURL(params.DownloadSrvAddr, externalIP)
	var blockDownload = &BlockDownload{
		limiter:        limiter,
		blockStore:     params.BlockStore,
		downloadSrvKey: params.DownloadSrvKey,
		downloadSrvURL: downloadSrvURL,
		scheduler:      params.Scheduler,
		deviceID:       deviceID}
	go blockDownload.startDownloadServer(params.DownloadSrvAddr)

	return blockDownload
}

func parseDownloadSrvURL(downloadSrvAddr, externalIP string) string {
	const unspecifiedAddress = "0.0.0.0"
	addressSlice := strings.Split(downloadSrvAddr, ":")
	if len(addressSlice) != 2 {
		log.Fatal("Invalid downloadSrvAddr")
	}

	if addressSlice[0] == unspecifiedAddress {
		return fmt.Sprintf("http://%s:%s%s", externalIP, addressSlice[1], helper.DownloadSrvPath)
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

	go bd.statistics(bd.deviceID, cidStr)

	log.Infof("Download block %s costTime %d, size %d, speed %d", cidStr, costTime, n, speedRate)

	return
}

func (bd *BlockDownload) statistics(deviceID, cid string) {
	bd.scheduler.DownloadBlockResult(context.Background(), deviceID, cid)
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

// set download server upload speed
func (bd *BlockDownload) SetDownloadSpeed(ctx context.Context, speedRate int64) error {
	log.Infof("set download speed %d", speedRate)
	if bd.limiter == nil {
		return fmt.Errorf("edge.limiter == nil")
	}
	bd.limiter.SetLimit(rate.Limit(speedRate))
	bd.limiter.SetBurst(int(speedRate))

	return nil
}

func (bd *BlockDownload) UnlimitDownloadSpeed() error {
	log.Debug("UnlimitDownloadSpeed")
	if bd.limiter == nil {
		return fmt.Errorf("edge.limiter == nil")
	}

	bd.limiter.SetLimit(rate.Inf)
	bd.limiter.SetBurst(0)

	return nil
}

func (bd *BlockDownload) GetDownloadInfo(ctx context.Context) (api.DownloadInfo, error) {
	tk, err := token.GenerateToken(bd.downloadSrvKey, time.Now().Add(helper.DownloadTokenExpireAfter).Unix())
	if err != nil {
		return api.DownloadInfo{}, err
	}

	info := api.DownloadInfo{
		URL:   bd.downloadSrvURL,
		Token: tk,
	}

	return info, nil

}

func (bd *BlockDownload) GetRateLimit() int64 {
	log.Debug("GenerateDownloadToken")
	return int64(bd.limiter.Limit())
}
