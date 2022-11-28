package download

import (
	"context"
	"crypto"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/pem"
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
	"github.com/linguohua/titan/node/device"
	"github.com/linguohua/titan/node/helper"
	"golang.org/x/time/rate"
)

var log = logging.Logger("download")

type BlockDownload struct {
	limiter    *rate.Limiter
	blockStore blockstore.BlockStore
	publicKey  *rsa.PublicKey
	scheduler  api.Scheduler
	device     *device.Device
	srvAddr    string
}

func NewBlockDownload(limiter *rate.Limiter, params *helper.NodeParams, device *device.Device) *BlockDownload {
	var blockDownload = &BlockDownload{
		limiter:    limiter,
		blockStore: params.BlockStore,
		scheduler:  params.Scheduler,
		srvAddr:    params.DownloadSrvAddr,
		device:     device}

	go blockDownload.startDownloadServer()

	return blockDownload
}

func (bd *BlockDownload) getBlock(w http.ResponseWriter, r *http.Request) {
	appName := r.Header.Get("App-Name")
	// sign := r.Header.Get("Sign")
	cidStr := r.URL.Query().Get("cid")
	sign := r.URL.Query().Get("sign")
	snStr := r.URL.Query().Get("sn")
	signTime := r.URL.Query().Get("signTime")
	timeout := r.URL.Query().Get("timeout")

	log.Infof("GetBlock, App-Name:%s, sign:%s, sn:%d  cid:%s", appName, sign, snStr, cidStr)

	sn, err := strconv.ParseInt(snStr, 10, 64)
	if err != nil {
		log.Errorf("Parser param sn(%s) error:%s", sn, err.Error())
		http.Error(w, fmt.Sprintf("Parser param sn(%s) error:%s", snStr, err.Error()), http.StatusBadRequest)
		return
	}

	content := cidStr + snStr + signTime + timeout
	hash := sha256.New()
	_, err = hash.Write([]byte(content))
	if err != nil {
		log.Errorf("Write content %s to hash error:%s", content, err.Error())
		http.Error(w, fmt.Sprintf("Write content %s to hash error:%s", content, err.Error()), http.StatusBadRequest)
		return
	}
	hashSum := hash.Sum(nil)

	_, err = verifyRsaSign(bd.publicKey, []byte(sign), hashSum)
	if err != nil {
		log.Errorf("Verify sign cid:%s, sign:%s,sn:%s,signTime:%s, timeout:%s, error:%s,", cidStr, sign, snStr, signTime, timeout, err.Error())
		http.Error(w, fmt.Sprintf("Write content %s to hash error:%s", content, err.Error()), http.StatusBadRequest)
		return
	}

	blockHash, err := helper.CIDString2HashString(cidStr)
	if err != nil {
		log.Errorf("Parser param cid(%s) error:%s", cidStr, err.Error())
		http.Error(w, fmt.Sprintf("Parser param cid(%s) error:%s", cidStr, err.Error()), http.StatusBadRequest)
		return
	}

	reader, err := bd.blockStore.GetReader(blockHash)
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

	go bd.downloadBlockResult([]byte(sign), bd.device.GetDeviceID(), sn, speedRate, getClientIP(r))

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

func (bd *BlockDownload) downloadBlockResult(sign []byte, deviceID string, sn, downloadSpeed int64, clientIP string) {
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

func (bd *BlockDownload) GetRateLimit() int64 {
	log.Debug("GenerateDownloadToken")
	return int64(bd.limiter.Limit())
}

func (bd *BlockDownload) GetDownloadSrvURL() string {
	addrSplit := strings.Split(bd.srvAddr, ":")
	url := fmt.Sprintf("http://%s:%s%s", bd.device.GetExternaIP(), addrSplit[1], helper.DownloadSrvPath)
	return url
}

func (bd *BlockDownload) LoadPublicKey() error {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	publicKeyStr, err := bd.scheduler.GetPublicKey(ctx)
	if err != nil {
		return err
	}

	bd.publicKey, err = pem2PublicKey(publicKeyStr)
	if err != nil {
		return err
	}
	return nil
}

func pem2PublicKey(publicKeyStr string) (*rsa.PublicKey, error) {
	block, _ := pem.Decode([]byte(publicKeyStr))
	if block == nil {
		return nil, fmt.Errorf("failed to decode public key")
	}

	pub, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("failed to parse public key")
	}

	return pub.(*rsa.PublicKey), nil
}

func verifyRsaSign(publicKey *rsa.PublicKey, sign []byte, digest []byte) (bool, error) {
	err := rsa.VerifyPKCS1v15(publicKey, crypto.SHA256, []byte(digest), sign)
	if err != nil {
		fmt.Println("could not verify signature: ", err)
		return false, err
	}
	return true, nil
}
