package candidate

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/filecoin-project/go-jsonrpc"
	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/api/client"
	"golang.org/x/time/rate"

	"github.com/linguohua/titan/node/carfile"
	"github.com/linguohua/titan/node/carfile/carfilestore"
	"github.com/linguohua/titan/node/carfile/downloader"
	"github.com/linguohua/titan/node/common"
	"github.com/linguohua/titan/node/device"
	"github.com/linguohua/titan/node/download"
	"github.com/linguohua/titan/node/helper"
	datasync "github.com/linguohua/titan/node/sync"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	logging "github.com/ipfs/go-log/v2"
	vd "github.com/linguohua/titan/node/validate"
	mh "github.com/multiformats/go-multihash"
)

var log = logging.Logger("candidate")

func NewLocalCandidateNode(ctx context.Context, tcpSrvAddr string, device *device.Device, params *CandidateParams) api.Candidate {
	rateLimiter := rate.NewLimiter(rate.Limit(device.GetBandwidthUp()), int(device.GetBandwidthUp()))

	validate := vd.NewValidate(params.CarfileStore, device)

	blockDownload := download.NewBlockDownload(rateLimiter, params.Scheduler, params.CarfileStore, device, validate)
	carfileOperation := carfile.NewCarfileOperation(params.DS, params.CarfileStore, params.Scheduler, downloader.NewIPFS(params.IPFSAPI, params.CarfileStore), device)

	candidate := &Candidate{
		Device:           device,
		CarfileOperation: carfileOperation,
		BlockDownload:    blockDownload,
		Validate:         validate,
		scheduler:        params.Scheduler,
		tcpSrvAddr:       tcpSrvAddr,
		DataSync:         datasync.NewDataSync(params.DS),
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
	*carfile.CarfileOperation
	*download.BlockDownload
	*device.Device
	*vd.Validate
	*datasync.DataSync

	scheduler      api.Scheduler
	tcpSrvAddr     string
	blockWaiterMap sync.Map
}

type CandidateParams struct {
	DS           datastore.Batching
	Scheduler    api.Scheduler
	CarfileStore *carfilestore.CarfileStore
	IPFSAPI      string
}

func (candidate *Candidate) WaitQuiet(ctx context.Context) error {
	log.Debug("WaitQuiet")
	return nil
}

func (candidate *Candidate) GetBlocksOfCarfile(ctx context.Context, carfileCID string, randomSeed int64, randomCount int) (map[int]string, error) {
	blockCount, err := candidate.CarfileOperation.BlockCountOfCarfile(carfileCID)
	if err != nil {
		log.Errorf("GetBlocksOfCarfile, BlockCountOfCarfile error:%s, carfileCID:%s", err.Error(), carfileCID)
		return nil, err
	}

	indexs := make([]int, 0)
	indexMap := make(map[int]struct{})
	r := rand.New(rand.NewSource(randomSeed))

	for i := 0; i < randomCount; i++ {
		index := r.Intn(blockCount)

		if _, ok := indexMap[index]; !ok {
			indexs = append(indexs, index)
			indexMap[index] = struct{}{}
		}
	}

	return candidate.CarfileOperation.GetBlocksOfCarfile(carfileCID, indexs)
}

func (candidate *Candidate) ValidateNodes(ctx context.Context, req []api.ReqValidate) error {
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
				}
				result.Cids = append(result.Cids, cid)
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
	result := &api.ValidateResults{CarfileCID: req.CarfileCID, RoundID: req.RoundID, RandomCount: 0, Cids: make([]string, 0)}

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

	deviceID, err := api.DeviceID(ctx)
	if err != nil {
		result.IsTimeout = true
		sendValidateResult(candidate, result)
		log.Errorf("validate get device info err: %v", err)
		return
	}

	result.DeviceID = deviceID

	bw, exist := candidate.loadBlockWaiterFromMap(deviceID)
	if exist {
		log.Errorf("Aready doing validate node, deviceID:%s, not need to repeat to do", deviceID)
		return
	}

	bw = &blockWaiter{conn: nil, ch: make(chan tcpMsg, 1)}
	candidate.blockWaiterMap.Store(deviceID, bw)

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
	DeviceID(ctx context.Context) (string, error)
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
