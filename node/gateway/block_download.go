package gateway

import (
	"bytes"
	"context"
	"crypto/rsa"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/linguohua/titan/api/types"
	"github.com/linguohua/titan/node/carfile/store"
	titanRsa "github.com/linguohua/titan/node/rsa"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/lib/limiter"
	"github.com/linguohua/titan/node/device"
	"github.com/linguohua/titan/node/validate"
	"golang.org/x/time/rate"
)

const (
	downloadPath        = "/block/get"
	schedulerApiTimeout = 3
)

type BlockDownload struct {
	limiter      *rate.Limiter
	carfileStore *store.CarfileStore
	publicKey    *rsa.PublicKey
	scheduler    api.Scheduler
	device       *device.Device
	validate     *validate.Validate
}

func NewBlockDownload(limiter *rate.Limiter, scheduler api.Scheduler, carfileStore *store.CarfileStore, device *device.Device, validate *validate.Validate) *BlockDownload {
	blockDownload := &BlockDownload{
		limiter:      limiter,
		carfileStore: carfileStore,
		scheduler:    scheduler,
		validate:     validate,
		device:       device,
	}

	http.HandleFunc(downloadPath, blockDownload.getBlock)
	return blockDownload
}

func (bd *BlockDownload) resultFailed(w http.ResponseWriter, r *http.Request, sn int64, sign []byte, cidStr string, err error) {
	log.Errorf("result failed:%s", err.Error())

	if sign != nil {
		result := types.UserDownloadResult{SN: sn, Sign: sign, DownloadSpeed: 0, BlockSize: 0, Succeed: false, FailedReason: err.Error(), BlockCID: cidStr}
		go bd.downloadBlockResult(result)
	}

	if err == datastore.ErrNotFound {
		http.NotFound(w, r)
		return
	}

	http.Error(w, err.Error(), http.StatusBadRequest)
}

func (bd *BlockDownload) getBlock(w http.ResponseWriter, r *http.Request) {
	appName := r.Header.Get("App-Name")
	// cidStr := r.URL.Query().Get("cid")
	signStr := r.URL.Query().Get("sign")
	snStr := r.URL.Query().Get("sn")
	signTime := r.URL.Query().Get("signTime")
	timeout := r.URL.Query().Get("timeout")

	paths := strings.Split(r.URL.Path, "/")
	if len(paths) < 2 {
		bd.resultFailed(w, r, 0, nil, "", fmt.Errorf("path %s error", r.URL.Path))
		return
	}

	cidStr := paths[1]

	log.Infof("GetBlock, App-Name:%s, sign:%s, sn:%s, signTime:%s, timeout:%s,  cid:%s", appName, signStr, snStr, signTime, timeout, cidStr)

	sn, err := strconv.ParseInt(snStr, 10, 64)
	if err != nil {
		bd.resultFailed(w, r, 0, nil, cidStr, fmt.Errorf("parser param sn(%s) error:%s", snStr, err.Error()))
		return
	}

	sign, err := hex.DecodeString(signStr)
	if err != nil {
		bd.resultFailed(w, r, 0, nil, cidStr, fmt.Errorf("DecodeString sign(%s) error:%s", signStr, err.Error()))
		return
	}

	nodeID, _ := bd.device.NodeID(context.Background())
	if bd.publicKey == nil {
		bd.resultFailed(w, r, sn, sign, cidStr, fmt.Errorf("node %s publicKey == nil", nodeID))
		return
	}

	content := nodeID + snStr + signTime + timeout
	err = titanRsa.VerifyRsaSign(bd.publicKey, sign, content)
	if err != nil {
		bd.resultFailed(w, r, sn, sign, cidStr, fmt.Errorf("verify sign cid:%s,sn:%s,signTime:%s, timeout:%s, error:%s,", cidStr, snStr, signTime, timeout, err.Error()))
		return
	}

	c, err := cid.Decode(cidStr)
	if err != nil {
		bd.resultFailed(w, r, 0, nil, "", err)
		return
	}

	blk, err := bd.carfileStore.Block(c)
	if err != nil {
		bd.resultFailed(w, r, sn, sign, cidStr, err)
		return
	}

	bd.validate.CancelValidate()

	contentDisposition := fmt.Sprintf("attachment; filename=%s", cidStr)
	w.Header().Set("Content-Disposition", contentDisposition)
	w.Header().Set("Content-Length", fmt.Sprintf("%d", len(blk.RawData())))

	now := time.Now()

	n, err := io.Copy(w, limiter.NewReader(bytes.NewReader(blk.RawData()), bd.limiter))
	if err != nil {
		log.Errorf("GetBlock, io.Copy error:%v", err)
		return
	}

	costTime := time.Since(now)

	speedRate := int64(0)
	if costTime != 0 {
		speedRate = int64(float64(n) / float64(costTime) * float64(time.Second))
	}

	result := types.UserDownloadResult{SN: sn, Sign: sign, DownloadSpeed: speedRate, BlockSize: int(n), Succeed: true, BlockCID: cidStr}
	go bd.downloadBlockResult(result)

	log.Infof("Download block %s costTime %d, size %d, speed %d", cidStr, costTime, n, speedRate)

}

func (bd *BlockDownload) downloadBlockResult(result types.UserDownloadResult) {
	ctx, cancel := context.WithTimeout(context.Background(), schedulerApiTimeout*time.Second)
	defer cancel()

	err := bd.scheduler.UserDownloadResult(ctx, result)
	if err != nil {
		log.Errorf("downloadBlockResult error:%s", err.Error())
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

func (bd *BlockDownload) GetRateLimit() int64 {
	log.Debug("GenerateDownloadToken")
	return int64(bd.limiter.Limit())
}
