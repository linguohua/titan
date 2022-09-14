package candidate

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/filecoin-project/go-jsonrpc"
	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/api/client"
	"github.com/linguohua/titan/build"
	"github.com/linguohua/titan/lib/p2p"
	"golang.org/x/time/rate"

	"github.com/linguohua/titan/node/block"
	"github.com/linguohua/titan/node/common"
	"github.com/linguohua/titan/node/device"
	"github.com/linguohua/titan/node/download"
	"github.com/linguohua/titan/node/edge"
	"github.com/linguohua/titan/node/helper"

	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	vd "github.com/linguohua/titan/node/validate"
	mh "github.com/multiformats/go-multihash"
)

var log = logging.Logger("candidate")

func NewLocalCandidateNode(ctx context.Context, tcpSrvAddr string, params *edge.EdgeParams) api.Candidate {
	addrs, err := build.BuiltinBootstrap()
	if err != nil {
		log.Fatal(err)
	}

	exchange, err := p2p.Bootstrap(ctx, addrs)
	if err != nil {
		log.Fatal(err)
	}

	rateLimiter := rate.NewLimiter(rate.Limit(params.Device.BandwidthUp), int(params.Device.BandwidthUp))
	blockDownload := download.NewBlockDownload(rateLimiter, params.BlockStore, params.DownloadSrvKey, params.DownloadSrvAddr, params.Device.InternalIP)
	params.Device.SetBlockDownload(blockDownload)

	block := block.NewBlock(params.DS, params.BlockStore, params.Scheduler, &block.IPFS{}, exchange, params.Device.DeviceID)

	validate := vd.NewValidate(blockDownload, block, params.Device.DeviceID)

	candidate := &Candidate{
		Device:        params.Device,
		Block:         block,
		BlockDownload: blockDownload,
		Validate:      validate,
		scheduler:     params.Scheduler,
	}

	go candidate.startTcpServer(tcpSrvAddr)
	return candidate
}

func cidFromData(data []byte) (string, error) {
	pref := cid.Prefix{
		Version:  1,
		Codec:    uint64(cid.Raw),
		MhType:   mh.SHA2_256,
		MhLength: -1, // default length
	}

	c, err := pref.Sum(data)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("%v", c), nil
}

type blockWaiter struct {
	conn *net.TCPConn
	ch   chan []byte
}

type Candidate struct {
	*common.CommonAPI
	*block.Block
	*download.BlockDownload
	*device.Device
	*vd.Validate

	scheduler      api.Scheduler
	tcpSrvAddr     string
	blockWaiterMap sync.Map
}

func (candidate *Candidate) WaitQuiet(ctx context.Context) error {
	log.Debug("WaitQuiet")
	return nil
}

func (candidate *Candidate) ValidateBlocks(ctx context.Context, req []api.ReqValidate) error {
	log.Debug("ValidateBlocks")

	for _, reqValidate := range req {
		param := reqValidate
		go validate(&param, candidate)
	}

	return nil
}

func (candidate *Candidate) loadBlockWaiterFromMap(key string) (*blockWaiter, bool) {
	vb, ok := candidate.blockWaiterMap.Load(key)
	if ok {
		return vb.(*blockWaiter), ok
	}
	return nil, ok
}

func sendValidateResult(ctx context.Context, candidate *Candidate, result *api.ValidateResults) error {
	return candidate.scheduler.ValidateBlockResult(ctx, *result)
}

func waitBlock(vb *blockWaiter, req *api.ReqValidate, candidate *Candidate, result *api.ValidateResults) {
	defer func() {
		vb, ok := candidate.loadBlockWaiterFromMap(result.DeviceID)
		if ok {
			candidate.blockWaiterMap.Delete(result.DeviceID)
			if vb.ch != nil {
				close(vb.ch)
				vb.ch = nil
			}
			if vb.conn != nil {
				vb.conn.Close()
			}
		}
	}()

	size := int64(0)
	now := time.Now()
	isBreak := false
	t := time.NewTimer(time.Duration(req.Duration+helper.ValidateTimeout) * time.Second)
	for {
		select {
		case block, ok := <-vb.ch:
			if !ok {
				// log.Infof("waitblock close channel %s", result.DeviceID)
				isBreak = true
				vb.ch = nil
				break
			}

			size += int64(len(block))
			cid, err := cidFromData(block)
			if err != nil {
				log.Errorf("waitBlock, cidFromData error:%v", err)
			} else {
				result.Cids[result.RandomCount] = cid
			}
			result.RandomCount++
		case <-t.C:
			log.Errorf("waitBlock timeout %ds, exit wait block", req.Duration+helper.ValidateTimeout)
			isBreak = true
		}

		if isBreak {
			break
		}

	}

	duration := time.Now().Sub(now)
	if duration < time.Duration(req.Duration)*time.Second {
		duration = time.Duration(req.Duration) * time.Second
	}

	result.Bandwidth = float64(size) / float64(duration) * float64(time.Second)
	result.CostTime = int(duration / time.Millisecond)

	log.Infof("validate %s %d block, bandwidth:%f, cost time:%d, IsTimeout:%v, duration:%d, size:%d", result.DeviceID, len(result.Cids), result.Bandwidth, result.CostTime, result.IsTimeout, req.Duration, size)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sendValidateResult(ctx, candidate, result)
}

func validate(req *api.ReqValidate, candidate *Candidate) {
	result := &api.ValidateResults{RoundID: req.RoundID, RandomCount: 0, Cids: make(map[int]string)}
	// result.Results = make([]api.ValidateResult, 0)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	api, closer, err := getNodeApi(req.NodeType, req.NodeURL)
	if err != nil {
		result.IsTimeout = true
		sendValidateResult(ctx, candidate, result)
		log.Errorf("validate get node api err: %v", err)
		return
	}
	defer closer()

	info, err := api.DeviceInfo(ctx)
	if err != nil {
		result.IsTimeout = true
		sendValidateResult(ctx, candidate, result)
		log.Errorf("validate get device info err: %v", err)
		return
	}

	result.DeviceID = info.DeviceId

	bw, ok := candidate.loadBlockWaiterFromMap(info.DeviceId)
	if ok {
		log.Errorf("Aready doing validate node, deviceID:%s, not need to repeat to do", info.DeviceId)
		return
	}

	bw = &blockWaiter{conn: nil, ch: make(chan []byte)}
	candidate.blockWaiterMap.Store(info.DeviceId, bw)

	go waitBlock(bw, req, candidate, result)

	wctx, cancel := context.WithTimeout(context.Background(), (time.Duration(req.Duration))*time.Second)
	defer cancel()

	err = api.BeValidate(wctx, *req, candidate.tcpSrvAddr)
	if err != nil {
		result.IsTimeout = true
		sendValidateResult(ctx, candidate, result)
		log.Errorf("validate, edge DoValidate err: %v", err)
		return
	}
}

type nodeAPI interface {
	DeviceInfo(ctx context.Context) (api.DevicesInfo, error)
	BeValidate(ctx context.Context, reqValidate api.ReqValidate, candidateTcpSrvAddr string) error
}

func getNodeApi(nodeType int, nodeURL string) (nodeAPI, jsonrpc.ClientCloser, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if nodeType == int(api.NodeEdge) {
		return client.NewEdge(ctx, nodeURL, nil)
	} else if nodeType == int(api.NodeCandidate) {
		return client.NewCandicate(ctx, nodeURL, nil)
	}

	return nil, nil, fmt.Errorf("NodeType %d not NodeEdge or NodeCandidate", nodeType)
}
