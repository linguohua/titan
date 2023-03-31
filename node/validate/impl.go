package validate

import (
	"context"
	"math/rand"
	"net"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-libipfs/blocks"
	logging "github.com/ipfs/go-log/v2"
	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/node/device"
	"golang.org/x/time/rate"
	"golang.org/x/xerrors"
)

var log = logging.Logger("validate")

type Validate struct {
	storage               Storage
	device                *device.Device
	cancelValidateChannel chan bool
}

type Storage interface {
	// GetCar get car root cid with randomSeed
	// randomSeed for random bucket
	GetCar(ctx context.Context, randomSeed int64) (cid.Cid, error)
	// GetCID get block cid of car with index
	GetCID(ctx context.Context, root cid.Cid, index int) (cid.Cid, error)
	// GetBlock get block content of car
	GetBlock(ctx context.Context, c cid.Cid) (blocks.Block, error)
	// get block count of car
	BlockCountOfCar(ctx context.Context, root cid.Cid) (uint32, error)
}

func NewValidate(storage Storage, device *device.Device) *Validate {
	return &Validate{storage: storage, device: device}
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

	r := rand.New(rand.NewSource(req.RandomSeed))
	t := time.NewTimer(time.Duration(req.Duration) * time.Second)
	limiter := rate.NewLimiter(rate.Limit(speedRate), int(speedRate))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	root, err := validate.storage.GetCar(ctx, req.RandomSeed)
	if err != nil {
		return xerrors.Errorf("get car error %w", err)
	}
	count, err := validate.storage.BlockCountOfCar(ctx, root)
	if err != nil {
		return err
	}

	nodeID, err := validate.device.NodeID(ctx)
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

		index := r.Intn(int(count))
		cid, err := validate.storage.GetCID(ctx, root, index)
		if err != nil {
			return err
		}

		blk, err := validate.storage.GetBlock(ctx, cid)
		if err != nil && err != datastore.ErrNotFound {
			return err
		}

		var block []byte
		if blk != nil {
			block = blk.RawData()
		}

		err = sendData(conn, block, api.TCPMsgTypeBlock, limiter)
		if err != nil {
			return err
		}
	}
}
