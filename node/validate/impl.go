package validate

import (
	"context"
	"math/rand"
	"net"
	"time"

	"github.com/ipfs/go-datastore"
	logging "github.com/ipfs/go-log/v2"
	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/node/carfile/carfilestore"
	"github.com/linguohua/titan/node/device"
	"golang.org/x/time/rate"
)

var log = logging.Logger("validate")

type Validate struct {
	carfileStore          *carfilestore.CarfileStore
	device                *device.Device
	cancelValidateChannel chan bool
}

func NewValidate(carfileStore *carfilestore.CarfileStore, device *device.Device) *Validate {
	return &Validate{carfileStore: carfileStore, device: device}
}

func (validate *Validate) BeValidate(ctx context.Context, reqValidate api.ReqValidate, candidateTcpSrvAddr string) error {
	log.Debug("BeValidate")

	conn, err := newTcpClient(candidateTcpSrvAddr)
	if err != nil {
		log.Errorf("BeValidate, NewCandicate err:%v", err)
		return err
	}

	go validate.sendBlocks(conn, &reqValidate, validate.device.GetBandwidthUp())

	return nil
}

func (validate *Validate) CancelValidate() {
	if validate.cancelValidateChannel != nil {
		validate.cancelValidateChannel <- true
	}
}
func (validate *Validate) sendBlocks(conn *net.TCPConn, reqValidate *api.ReqValidate, speedRate int64) {
	defer func() {
		validate.cancelValidateChannel = nil
		conn.Close()
	}()

	validate.cancelValidateChannel = make(chan bool)

	r := rand.New(rand.NewSource(reqValidate.Seed))
	t := time.NewTimer(time.Duration(reqValidate.Duration) * time.Second)

	limiter := rate.NewLimiter(rate.Limit(speedRate), int(speedRate))

	sendDeviceID(conn, validate.device.GetDeviceID(), limiter)
	for {
		select {
		case <-t.C:
			return
		case <-validate.cancelValidateChannel:
			err := sendData(conn, nil, api.ValidateTcpMsgTypeCancelValidate, limiter)
			if err != nil {
				log.Errorf("sendBlocks, send cancel validate error:%v", err)
			}
			return
		default:
		}

		fid := r.Intn(reqValidate.MaxFid) + 1
		blockHashs, err := validate.carfileStore.GetBlocksHashWithCarfilePositions("", []int{fid})
		if err != nil && err != datastore.ErrNotFound {
			log.Errorf("sendBlocks, get blockHashs error:%v", err)
			return
		}

		var block []byte
		if len(blockHashs) > 0 {
			block, err = validate.carfileStore.GetBlock(blockHashs[0])
			if err != nil && err != datastore.ErrNotFound {
				log.Errorf("sendBlocks, get block error:%v", err)
				return
			}
		}

		err = sendData(conn, block, api.ValidateTcpMsgTypeBlockContent, limiter)
		if err != nil {
			log.Errorf("sendBlocks, send data error:%v", err)
			return
		}
	}
}
