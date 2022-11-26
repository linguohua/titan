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
	"github.com/linguohua/titan/node/device"
	"github.com/linguohua/titan/node/helper"
	"golang.org/x/time/rate"
)

var log = logging.Logger("download")

type BlockDownload struct {
	limiter        *rate.Limiter
	blockStore     blockstore.BlockStore
	downloadSrvKey string
	scheduler      api.Scheduler
	device         *device.Device
	srvAddr        string
}

func NewBlockDownload(limiter *rate.Limiter, params *helper.NodeParams, device *device.Device) *BlockDownload {
	var blockDownload = &BlockDownload{
		limiter:        limiter,
		blockStore:     params.BlockStore,
		downloadSrvKey: params.DownloadSrvKey,
		scheduler:      params.Scheduler,
		srvAddr:        params.DownloadSrvAddr,
		device:         device}

	go blockDownload.startDownloadServer()

	return blockDownload
}

func (bd *BlockDownload) getBlock(w http.ResponseWriter, r *http.Request) {
	appName := r.Header.Get("App-Name")
	// sign := r.Header.Get("Sign")
	cidStr := r.URL.Query().Get("cid")
	sign := r.URL.Query().Get("sign")
	snStr := r.URL.Query().Get("sn")

	log.Infof("GetBlock, App-Name:%s, sign:%s, sn:%d  cid:%s", appName, sign, snStr, cidStr)

	// TODO: need to verify sign

	sn, err := strconv.ParseInt(snStr, 10, 64)
	if err != nil {
		log.Errorf("Parser param sn(%s) error:%s", sn, err.Error())
		http.Error(w, fmt.Sprintf("Parser param sn(%s) error:%s", snStr, err.Error()), http.StatusBadRequest)
		return
	}

	hash, err := helper.CIDString2HashString(cidStr)
	if err != nil {
		log.Errorf("Parser param cid(%s) error:%s", cidStr, err.Error())
		http.Error(w, fmt.Sprintf("Parser param cid(%s) error:%s", cidStr, err.Error()), http.StatusBadRequest)
		return
	}

	reader, err := bd.blockStore.GetReader(hash)
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

	go bd.downloadBlockResult(sign, bd.device.GetDeviceID(), 0, speedRate, getClientIP(r))

	log.Infof("Download block %s costTime %d, size %d, speed %d", cidStr, costTime, n, speedRate)

	return
}

func getClientIP(r *http.Request) string {
	reqIP := r.Header.Get("X-Real-IP")
	if reqIP == "" {
		h, _, err := net.SplitHostPort(r.RemoteAddr)
		if err != nil {
			log.Errorf("could not get ip from: %s, err: %s", r.RemoteAddr, err)
		}
		reqIP = h
	}

	return reqIP
}

func (bd *BlockDownload) downloadBlockResult(sign, deviceID string, sn, downloadSpeed int64, clientIP string) {
	result := api.NodeBlockDownloadResult{SN: sn, Sign: sign, DownloadSpeed: downloadSpeed, ClientIP: clientIP}
	bd.scheduler.NodeDownloadBlockResult(context.Background(), result)
}

func (bd *BlockDownload) startDownloadServer() {
	mux := http.NewServeMux()
	mux.HandleFunc(helper.DownloadSrvPath, bd.getBlock)

	srv := &http.Server{
		Handler: mux,
		Addr:    bd.srvAddr,
	}

	nl, err := net.Listen("tcp", bd.srvAddr)
	if err != nil {
		log.Fatal(err)
	}

	log.Infof("download server listen on %s", bd.srvAddr)

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
	bd.device.SetBandwidthUp(speedRate)
	return nil
}

func (bd *BlockDownload) UnlimitDownloadSpeed() error {
	log.Debug("UnlimitDownloadSpeed")
	if bd.limiter == nil {
		return fmt.Errorf("edge.limiter == nil")
	}

	bd.limiter.SetLimit(rate.Inf)
	bd.limiter.SetBurst(0)
	bd.device.SetBandwidthUp(int64(bd.limiter.Limit()))
	return nil
}

func (bd *BlockDownload) GetDownloadInfo(ctx context.Context) (api.DownloadInfo, error) {
	tk, err := token.GenerateToken(bd.downloadSrvKey, time.Now().Add(helper.DownloadTokenExpireAfter).Unix())
	if err != nil {
		return api.DownloadInfo{}, err
	}

	addrSplit := strings.Split(bd.srvAddr, ":")
	info := api.DownloadInfo{
		URL:   fmt.Sprintf("http://%s:%s%s", bd.device.GetExternaIP(), addrSplit[1], helper.DownloadSrvPath),
		Token: tk,
	}

	return info, nil
}

func (bd *BlockDownload) GetRateLimit() int64 {
	log.Debug("GenerateDownloadToken")
	return int64(bd.limiter.Limit())
}

func (bd *BlockDownload) UpdateDownloadServerAccessAuth(exteranlIP string) error {
	addrSplit := strings.Split(bd.srvAddr, ":")
	url := fmt.Sprintf("http://%s:%s%s", exteranlIP, addrSplit[1], helper.DownloadSrvPath)
	accessAuth := api.DownloadServerAccessAuth{DeviceID: bd.device.GetDeviceID(), URL: url, SecurityKey: bd.downloadSrvKey}
	return bd.scheduler.UpdateDownloadServerAccessAuth(context.Background(), accessAuth)
}
