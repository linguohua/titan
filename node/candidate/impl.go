package candidate

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"sync"
	"time"

	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/api/client"
	"github.com/linguohua/titan/build"
	"github.com/linguohua/titan/lib/p2p"
	"golang.org/x/time/rate"

	"github.com/linguohua/titan/node/base"
	"github.com/linguohua/titan/node/block"
	"github.com/linguohua/titan/node/device"
	"github.com/linguohua/titan/node/download"
	"github.com/linguohua/titan/node/edge"

	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	vd "github.com/linguohua/titan/node/validate"
	mh "github.com/multiformats/go-multihash"
)

var log = logging.Logger("candidate")

const validateTimeout = 5

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

	base := base.NewBase(block, blockDownload)

	validate := vd.NewValidate(blockDownload, block, params.Device.DeviceID)

	candidate := &Candidate{
		Device:   params.Device,
		Base:     base,
		Validate: validate,
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

type validateBlock struct {
	conn *net.TCPConn
	ch   chan []byte
}

type Candidate struct {
	api.Common
	api.Base
	*device.Device
	api.Validate

	scheduler   api.Scheduler
	tcpSrvAddr  string
	validateMap sync.Map
}

func (candidate *Candidate) WaitQuiet(ctx context.Context) error {
	log.Debug("WaitQuiet")
	return nil
}

// edge node send block to candidate
func (candidate *Candidate) SendBlock(ctx context.Context, block []byte, deviceID string) error {
	log.Infof("SendBlock, len:%d", len(block))
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

func (candidate *Candidate) loadValidateBlockFromMap(key string) (*validateBlock, bool) {
	vb, ok := candidate.validateMap.Load(key)
	if ok {
		return vb.(*validateBlock), ok
	}
	return nil, ok
}

func sendValidateResult(ctx context.Context, candidate *Candidate, result *api.ValidateResults) error {
	return candidate.scheduler.ValidateBlockResult(ctx, *result)
}

func toValidateResult(data []byte) (api.ValidateResult, error) {
	result := api.ValidateResult{}
	if len(data) == 0 {
		return result, fmt.Errorf("len(data) == 0")
	}

	cid, err := cidFromData(data)
	if err != nil {
		return result, fmt.Errorf("toValidateResult err: %v", err)
	}
	result.Cid = cid

	return result, nil
}

func waitBlock(vb *validateBlock, req *api.ReqValidate, candidate *Candidate, result *api.ValidateResults) {
	defer func() {
		vb, ok := candidate.loadValidateBlockFromMap(result.DeviceID)
		if ok {
			candidate.validateMap.Delete(result.DeviceID)
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
	t := time.NewTimer(time.Duration(req.Duration+validateTimeout) * time.Second)
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
			rs, _ := toValidateResult(block)
			result.Results = append(result.Results, rs)
		case <-t.C:
			log.Errorf("waitBlock timeout %ds, exit wait block", req.Duration+validateTimeout)
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

	r := rand.New(rand.NewSource(req.Seed))
	results := make([]api.ValidateResult, 0, len(result.Results))

	for _, rs := range result.Results {
		random := r.Intn(len(req.FIDs))
		rs.Fid = req.FIDs[random]
		results = append(results, rs)
	}

	result.Results = results

	log.Infof("validate %s %d block, bandwidth:%f, cost time:%d, IsTimeout:%v, duration:%d, size:%d", result.DeviceID, len(result.Results), result.Bandwidth, result.CostTime, result.IsTimeout, req.Duration, size)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sendValidateResult(ctx, candidate, result)
}

func validate(req *api.ReqValidate, candidate *Candidate) {
	result := &api.ValidateResults{RoundID: req.RoundID}
	result.Results = make([]api.ValidateResult, 0)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if len(req.FIDs) == 0 {
		sendValidateResult(ctx, candidate, result)
		log.Errorf("len(req.FIDs) == 0 ")
		return
	}

	edgeAPI, closer, err := client.NewEdge(ctx, req.EdgeURL, nil)
	if err != nil {
		result.IsTimeout = true
		sendValidateResult(ctx, candidate, result)
		log.Errorf("validate NewEdge err: %v", err)
		return
	}
	defer closer()

	info, err := edgeAPI.DeviceInfo(ctx)
	if err != nil {
		result.IsTimeout = true
		sendValidateResult(ctx, candidate, result)
		log.Errorf("validate get device info err: %v", err)
		return
	}

	result.DeviceID = info.DeviceId

	vb, ok := candidate.loadValidateBlockFromMap(info.DeviceId)
	if ok {
		log.Errorf("Aready doing validate edge node, deviceID:%s, not need to repeat to do", info.DeviceId)
		return
	}

	vb = &validateBlock{conn: nil, ch: make(chan []byte)}
	candidate.validateMap.Store(info.DeviceId, vb)

	go waitBlock(vb, req, candidate, result)

	wctx, cancel := context.WithTimeout(context.Background(), (time.Duration(req.Duration))*time.Second)
	defer cancel()

	err = edgeAPI.BeValidate(wctx, *req, candidate.tcpSrvAddr)
	if err != nil {
		result.IsTimeout = true
		sendValidateResult(ctx, candidate, result)
		log.Errorf("validate, edge DoValidate err: %v", err)
		return
	}
}
