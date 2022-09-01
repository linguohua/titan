package candidate

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/api/client"

	"github.com/linguohua/titan/node/edge"

	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	mh "github.com/multiformats/go-multihash"
)

var (
	log             = logging.Logger("candidate")
	validateTimeout = 5
	validateMap     sync.Map
)

type validateBlock struct {
	conn *net.TCPConn
	ch   chan []byte
}

func loadValidateBlockFromMap(key string) (*validateBlock, bool) {
	vb, ok := validateMap.Load(key)
	if ok {
		return vb.(*validateBlock), ok
	}
	return nil, ok
}

func NewLocalCandidateNode(ctx context.Context, tcpSrvAddr string, edgeParams *edge.EdgeParams) api.Candidate {
	a := edge.NewLocalEdgeNode(ctx, edgeParams)
	edge := a.(*edge.Edge)

	go startTcpServer(tcpSrvAddr)
	return &Candidate{Edge: *edge, tcpSrvAddr: parseTcpSrvAddr(tcpSrvAddr, edge.InternalIP)}
}

func parseTcpSrvAddr(tcpSrvAddr string, interalIP string) string {
	const unspecifiedAddress = "0.0.0.0"
	addressSlice := strings.Split(tcpSrvAddr, ":")
	if len(addressSlice) != 2 {
		log.Fatal("Invalid downloadSrvAddr")
	}

	if addressSlice[0] == unspecifiedAddress {
		return fmt.Sprintf("%s:%s", interalIP, addressSlice[1])
	}

	return tcpSrvAddr
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

type Candidate struct {
	edge.Edge
	tcpSrvAddr string
}

func (candidate *Candidate) WaitQuiet(ctx context.Context) error {
	log.Info("WaitQuiet")
	return nil
}

// edge node send block to candidate
func (candidate *Candidate) SendBlock(ctx context.Context, block []byte, deviceID string) error {
	log.Infof("SendBlock, len:%d", len(block))
	return nil
}

func (candidate *Candidate) ValidateData(ctx context.Context, req []api.ReqValidate) error {
	log.Info("ValidateData")

	for _, reqValidate := range req {
		param := reqValidate
		go validate(&param, candidate)
	}

	return nil
}

func sendValidateResult(ctx context.Context, candidate *Candidate, result *api.ValidateResults) error {
	scheduler := candidate.Edge.GetSchedulerAPI()
	return scheduler.ValidateDataResult(ctx, *result)
}

func toValidateResult(data []byte) (api.ValidateResult, error) {
	result := api.ValidateResult{}
	if len(data) == 0 {
		return result, fmt.Errorf("len(data) == 0")
	}

	cid, err := cidFromData(data)
	if err != nil {
		return result, fmt.Errorf("toValidateResult err : %v", err)
	}
	result.Cid = cid

	return result, nil

}

func waitBlock(vb *validateBlock, req *api.ReqValidate, candidate *Candidate, result *api.ValidateResults) {
	defer func() {
		vb, ok := loadValidateBlockFromMap(result.DeviceID)
		if ok {
			validateMap.Delete(result.DeviceID)
			if vb.ch != nil {
				close(vb.ch)
				vb.ch = nil
			}
			if vb.conn != nil {
				vb.conn.Close()
			}
		}
	}()

	var size = int64(0)
	var now = time.Now()
	var isBreak = false
	var t = time.NewTimer(time.Duration(req.Duration+validateTimeout) * time.Second)
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
		log.Errorf("validate NewEdge err : %v", err)
		return
	}
	defer closer()

	info, err := edgeAPI.DeviceInfo(ctx)
	if err != nil {
		result.IsTimeout = true
		sendValidateResult(ctx, candidate, result)
		log.Errorf("validate get device info err : %v", err)
		return
	}

	result.DeviceID = info.DeviceId

	vb, ok := validateMap.Load(info.DeviceId)
	if ok {
		log.Errorf("Aready doing validate edge node, deviceID:%s, not need to repeat to do", info.DeviceId)
		return
	}

	vb = &validateBlock{conn: nil, ch: make(chan []byte)}
	validateMap.Store(info.DeviceId, vb)

	go waitBlock(vb.(*validateBlock), req, candidate, result)

	wctx, cancel := context.WithTimeout(context.Background(), (time.Duration(req.Duration))*time.Second)
	defer cancel()

	err = edgeAPI.DoValidate(wctx, *req, candidate.tcpSrvAddr)
	if err != nil {
		result.IsTimeout = true
		sendValidateResult(ctx, candidate, result)
		log.Errorf("validate, edge DoValidate err : %v", err)
		return
	}
}
