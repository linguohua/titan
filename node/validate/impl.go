package validate

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"time"

	"github.com/ipfs/go-datastore"
	logging "github.com/ipfs/go-log/v2"
	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/node/block"
	"github.com/linguohua/titan/node/download"
	"golang.org/x/time/rate"
)

var log = logging.Logger("validate")

type Validate struct {
	blockDownload *download.BlockDownload
	block         *block.Block
	deviceID      string
}

func NewValidate(blockDownload *download.BlockDownload, block *block.Block, deviceID string) *Validate {
	return &Validate{blockDownload: blockDownload, block: block, deviceID: deviceID}
}

func (validate *Validate) BeValidate(ctx context.Context, reqValidate api.ReqValidate, candidateTcpSrvAddr string) error {
	log.Debug("BeValidate")

	oldRate := validate.limitBlockUploadRate()
	defer validate.resetBlockUploadRate(oldRate)

	conn, err := newTcpClient(candidateTcpSrvAddr)
	if err != nil {
		log.Errorf("DoValidate, NewCandicate err:%v", err)
		return err
	}

	go validate.sendBlocks(conn, &reqValidate, oldRate)

	return nil
}

func (validate *Validate) limitBlockUploadRate() int64 {
	oldRate := validate.blockDownload.GetRateLimit()
	validate.blockDownload.SetDownloadSpeed(context.TODO(), 0)
	return int64(oldRate)
}

func (validate *Validate) resetBlockUploadRate(oldRate int64) {
	validate.blockDownload.SetDownloadSpeed(context.TODO(), oldRate)
}

func (validate *Validate) sendBlocks(conn *net.TCPConn, reqValidate *api.ReqValidate, speedRate int64) {
	defer conn.Close()

	r := rand.New(rand.NewSource(reqValidate.Seed))
	t := time.NewTimer(time.Duration(reqValidate.Duration) * time.Second)

	limiter := rate.NewLimiter(rate.Limit(speedRate), int(speedRate))

	sendDeviceID(conn, validate.deviceID, limiter)
	for {
		select {
		case <-t.C:
			return
		default:
		}

		fid := r.Intn(reqValidate.MaxFid) + 1
		block, err := validate.block.LoadBlockWithFid(fmt.Sprintf("%d", fid))
		if err != nil && err != datastore.ErrNotFound {
			log.Errorf("sendBlocks, get block error:%v", err)
			return
		}

		err = sendData(conn, block, limiter)
		if err != nil {
			log.Errorf("sendBlocks, send data error:%v", err)
			return
		}
	}
}
