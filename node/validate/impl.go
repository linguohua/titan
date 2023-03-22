package validate

import (
	"context"
	"math/rand"
	"net"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	logging "github.com/ipfs/go-log/v2"
	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/node/carfile/store"
	"github.com/linguohua/titan/node/device"
	"golang.org/x/time/rate"
)

var log = logging.Logger("validate")

type Validate struct {
	carfileStore          *store.CarfileStore
	device                *device.Device
	cancelValidateChannel chan bool
}

func NewValidate(carfileStore *store.CarfileStore, device *device.Device) *Validate {
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

	r := rand.New(rand.NewSource(reqValidate.RandomSeed))
	t := time.NewTimer(time.Duration(reqValidate.Duration) * time.Second)

	limiter := rate.NewLimiter(rate.Limit(speedRate), int(speedRate))

	c, err := cid.Decode(reqValidate.CarfileCID)
	if err != nil {
		log.Errorf("sendBlocks, decode cid %s error %s", reqValidate.CarfileCID, err.Error())
		return
	}

	cids, err := validate.carfileStore.BlocksOfCarfile(c)
	if err != nil {
		log.Errorf("sendBlocks, BlocksCountOfCarfile error:%s, carfileCID:%s", err.Error(), reqValidate.CarfileCID)
		return
	}

	if len(cids) == 0 {
		log.Errorf("sendBlocks, carfile %s no block exist", reqValidate.CarfileCID)
		return
	}

	nodeID, err := validate.device.NodeID(context.Background())
	if err != nil {
		log.Errorf("sendBlocks, get nodeID error:%s", err.Error())
		return
	}

	sendNodeID(conn, nodeID, limiter)
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

		var block []byte
		index := r.Intn(len(cids))
		blk, err := validate.carfileStore.Block(cids[index])
		if err != nil && err != datastore.ErrNotFound {
			log.Errorf("sendBlocks, get block error:%v", err)
			return
		}

		if blk != nil {
			block = blk.RawData()
		}

		err = sendData(conn, block, api.ValidateTcpMsgTypeBlockContent, limiter)
		if err != nil {
			log.Errorf("sendBlocks, send data error:%v", err)
			return
		}
	}
}
