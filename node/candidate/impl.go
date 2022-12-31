package candidate

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/filecoin-project/go-jsonrpc"
	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/api/client"
	"golang.org/x/time/rate"

	"github.com/linguohua/titan/node/block"
	"github.com/linguohua/titan/node/common"
	"github.com/linguohua/titan/node/device"
	"github.com/linguohua/titan/node/download"
	"github.com/linguohua/titan/node/helper"
	datasync "github.com/linguohua/titan/node/sync"

	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	vd "github.com/linguohua/titan/node/validate"
	mh "github.com/multiformats/go-multihash"
)

var log = logging.Logger("candidate")

func NewLocalCandidateNode(ctx context.Context, tcpSrvAddr string, device *device.Device, params *helper.NodeParams) api.Candidate {
	rateLimiter := rate.NewLimiter(rate.Limit(device.GetBandwidthUp()), int(device.GetBandwidthUp()))

	block := block.NewBlock(params.DS, params.BlockStore, params.Scheduler, &block.IPFS{}, device, params.IPFSAPI)
	validate := vd.NewValidate(block, device)
	blockDownload := download.NewBlockDownload(rateLimiter, params, device, validate)

	datasync.SyncLocalBlockstore(params.DS, params.BlockStore, block)

	candidate := &Candidate{
		Device:        device,
		Block:         block,
		BlockDownload: blockDownload,
		Validate:      validate,
		scheduler:     params.Scheduler,
		tcpSrvAddr:    tcpSrvAddr,
		DataSync:      datasync.NewDataSync(block, params.DS),
	}

	go candidate.startTcpServer()
	return candidate
}

func cidFromData(data []byte) (string, error) {
	if len(data) == 0 {
		return "", fmt.Errorf("len(data) == 0")
	}

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

	return c.String(), nil
}

type blockWaiter struct {
	conn *net.TCPConn
	ch   chan tcpMsg
}

type Candidate struct {
	*common.CommonAPI
	*block.Block
	*download.BlockDownload
	*device.Device
	*vd.Validate
	*datasync.DataSync

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
	vb, exist := candidate.blockWaiterMap.Load(key)
	if exist {
		return vb.(*blockWaiter), exist
	}
	return nil, exist
}

func sendValidateResult(candidate *Candidate, result *api.ValidateResults) error {
	ctx, cancel := context.WithTimeout(context.Background(), helper.SchedulerApiTimeout*time.Second)
	defer cancel()

	return candidate.scheduler.ValidateBlockResult(ctx, *result)
}

func waitBlock(vb *blockWaiter, req *api.ReqValidate, candidate *Candidate, result *api.ValidateResults) {
	defer func() {
		candidate.blockWaiterMap.Delete(result.DeviceID)
	}()

	size := int64(0)
	now := time.Now()
	isBreak := false
	t := time.NewTimer(time.Duration(req.Duration+helper.ValidateTimeout) * time.Second)
	for {
		select {
		case tcpMsg, ok := <-vb.ch:
			if !ok {
				// log.Infof("waitblock close channel %s", result.DeviceID)
				isBreak = true
				vb.ch = nil
				break
			}

			if tcpMsg.msgType == api.ValidateTcpMsgTypeCancelValidate {
				result.IsCancel = true
				sendValidateResult(candidate, result)
				log.Infof("device %s cancel validate", result.DeviceID)
				return
			}

			if tcpMsg.msgType == api.ValidateTcpMsgTypeBlockContent && len(tcpMsg.msg) > 0 {
				cid, err := cidFromData(tcpMsg.msg)
				if err != nil {
					log.Errorf("waitBlock, cidFromData error:%v", err)
				} else {
					result.Cids[result.RandomCount] = cid
				}
			}
			size += int64(tcpMsg.length)
			result.RandomCount++
		case <-t.C:
			if vb.conn != nil {
				vb.conn.Close()
			}
			isBreak = true
			log.Errorf("wait device %s timeout %ds, exit wait block", result.DeviceID, req.Duration+helper.ValidateTimeout)
		}

		if isBreak {
			break
		}

	}

	duration := time.Now().Sub(now)
	result.CostTime = int64(duration / time.Millisecond)

	if duration < time.Duration(req.Duration)*time.Second {
		duration = time.Duration(req.Duration) * time.Second
	}
	result.Bandwidth = float64(size) / float64(duration) * float64(time.Second)

	log.Infof("validate %s %d block, bandwidth:%f, cost time:%d, IsTimeout:%v, duration:%d, size:%d, randCount:%d",
		result.DeviceID, len(result.Cids), result.Bandwidth, result.CostTime, result.IsTimeout, req.Duration, size, result.RandomCount)

	sendValidateResult(candidate, result)
}

func validate(req *api.ReqValidate, candidate *Candidate) {
	result := &api.ValidateResults{RoundID: req.RoundID, RandomCount: 0, Cids: make(map[int]string)}

	api, closer, err := getNodeApi(req.NodeType, req.NodeURL)
	if err != nil {
		result.IsTimeout = true
		sendValidateResult(candidate, result)
		log.Errorf("validate get node api err: %v", err)
		return
	}
	defer closer()

	ctx, cancel := context.WithTimeout(context.Background(), helper.SchedulerApiTimeout*time.Second)
	defer cancel()

	info, err := api.DeviceInfo(ctx)
	if err != nil {
		result.IsTimeout = true
		sendValidateResult(candidate, result)
		log.Errorf("validate get device info err: %v", err)
		return
	}

	result.DeviceID = info.DeviceId

	bw, exist := candidate.loadBlockWaiterFromMap(info.DeviceId)
	if exist {
		log.Errorf("Aready doing validate node, deviceID:%s, not need to repeat to do", info.DeviceId)
		return
	}

	bw = &blockWaiter{conn: nil, ch: make(chan tcpMsg, 1)}
	candidate.blockWaiterMap.Store(info.DeviceId, bw)

	go waitBlock(bw, req, candidate, result)

	wctx, cancel := context.WithTimeout(context.Background(), (time.Duration(req.Duration))*time.Second)
	defer cancel()

	addrSplit := strings.Split(candidate.tcpSrvAddr, ":")
	candidateTcpSrvAddr := fmt.Sprintf("%s:%s", candidate.GetExternaIP(), addrSplit[1])
	err = api.BeValidate(wctx, *req, candidateTcpSrvAddr)
	if err != nil {
		result.IsTimeout = true
		sendValidateResult(candidate, result)
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
