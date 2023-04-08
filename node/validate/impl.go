package validate

import (
	"context"
	"net"
	"time"

	"github.com/ipfs/go-libipfs/blocks"
	logging "github.com/ipfs/go-log/v2"
	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/node/device"
	"golang.org/x/time/rate"
	"golang.org/x/xerrors"
)

var log = logging.Logger("validate")

type Validate struct {
	checker               Checker
	device                *device.Device
	cancelValidateChannel chan bool
}

type RandomChecker interface {
	GetBlock(ctx context.Context) (blocks.Block, error)
}
type Checker interface {
	GetChecker(ctx context.Context, randomSeed int64) (RandomChecker, error)
}

func NewValidate(c Checker, device *device.Device) *Validate {
	return &Validate{checker: c, device: device}
}

func (validate *Validate) BeValidate(ctx context.Context, req *api.BeValidateReq) error {
	log.Debug("BeValidate")

	conn, err := newTCPClient(req.TCPSrvAddr)
	if err != nil {
		log.Errorf("BeValidate, NewCandicate err:%v", err)
		return err
	}

	go validate.sendBlocks(conn, req, validate.device.GetBandwidthUp())

	return nil
}

func (validate *Validate) CancelValidate() {
	if validate.cancelValidateChannel != nil {
		validate.cancelValidateChannel <- true
	}
}

func (validate *Validate) sendBlocks(conn *net.TCPConn, req *api.BeValidateReq, speedRate int64) error {
	defer func() {
		validate.cancelValidateChannel = nil
		if err := conn.Close(); err != nil {
			log.Errorf("close tcp error: %s", err.Error())
		}
	}()

	validate.cancelValidateChannel = make(chan bool)

	t := time.NewTimer(time.Duration(req.Duration) * time.Second)
	limiter := rate.NewLimiter(rate.Limit(speedRate), int(speedRate))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	checker, err := validate.checker.GetChecker(ctx, req.RandomSeed)
	if err != nil {
		return xerrors.Errorf("get car error %w", err)
	}

	nodeID, err := validate.device.GetNodeID(ctx)
	if err != nil {
		return err
	}

	if err := sendNodeID(conn, nodeID, limiter); err != nil {
		return err
	}

	for {
		select {
		case <-t.C:
			return nil
		case <-validate.cancelValidateChannel:
			err := sendData(conn, nil, api.TCPMsgTypeCancel, limiter)
			if err != nil {
				log.Errorf("send data error:%v", err)
			}
			return xerrors.Errorf("cancle validate")
		default:
		}

		blk, err := checker.GetBlock(ctx)
		if err != nil {
			return err
		}
		err = sendData(conn, blk.RawData(), api.TCPMsgTypeBlock, limiter)
		if err != nil {
			return err
		}
	}
}
