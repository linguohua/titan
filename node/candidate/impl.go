package candidate

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"strings"
	"time"

	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/api/client"

	"github.com/linguohua/titan/node/edge"

	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	mh "github.com/multiformats/go-multihash"
)

var log = logging.Logger("candidate")

type verifyBlock struct {
	conn *net.TCPConn
	ch   chan []byte
}

var verifyMap = make(map[string]*verifyBlock)

func NewLocalCandidateNode(ctx context.Context, tcpSrvAddr string, edgeParams edge.EdgeParams) api.Candidate {
	a := edge.NewLocalEdgeNode(ctx, edgeParams)
	edgeAPI := a.(edge.EdgeAPI)

	go startTcpServer(tcpSrvAddr)
	return CandidateAPI{EdgeAPI: edgeAPI, tcpSrvAddr: parseTcpSrvAddr(tcpSrvAddr, edgeAPI.InternalIP)}
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

type CandidateAPI struct {
	edge.EdgeAPI
	tcpSrvAddr string
}

func (candidate CandidateAPI) WaitQuiet(ctx context.Context) error {
	log.Info("WaitQuiet")
	return nil
}

// edge node send block to candidate
func (candidate CandidateAPI) SendBlock(ctx context.Context, block []byte, deviceID string) error {
	log.Infof("SendBlock, len:%d", len(block))
	// if deviceID == "" {
	// 	return fmt.Errorf("deviceID is empty")
	// }

	// ch, ok := verifyChannelMap[deviceID]
	// if !ok {
	// 	return fmt.Errorf("Candidate no wait for verify block")
	// }

	// result := verifyBlock{deviceID: deviceID, data: block}
	// ch <- result
	return nil
}

func (candidate CandidateAPI) VerifyData(ctx context.Context, req []api.ReqVerify) error {
	log.Info("VerifyData")

	for _, reqVerify := range req {
		go verify(reqVerify, candidate)
	}

	return nil
}

func sendVerifyResult(ctx context.Context, candidate CandidateAPI, result api.VerifyResults) error {
	scheduler := candidate.EdgeAPI.GetSchedulerAPI()
	return scheduler.VerifyDataResult(ctx, result)
}

func toVerifyResult(data []byte) api.VerifyResult {
	result := api.VerifyResult{}
	if len(data) == 0 {
		return result
	}

	cid, err := cidFromData(data)
	if err != nil {
		log.Errorf("toVerifyResult err : %v", err)
	} else {
		result.Cid = cid
	}

	return result

}

func waitBlock(vb *verifyBlock, req api.ReqVerify, candidate CandidateAPI, result api.VerifyResults) {
	defer func() {
		vb, ok := verifyMap[result.DeviceID]
		if ok {
			delete(verifyMap, result.DeviceID)
			if vb.ch != nil {
				close(vb.ch)
			}

			if vb.conn != nil {
				vb.conn.Close()
			}
		}
	}()

	var size = int64(0)
	var now = time.Now()
	var isBreak = false
	// add more 5 second to timeout
	var addMoreTimeout = 5
	var t = time.NewTimer(time.Duration(req.Duration+addMoreTimeout) * time.Second)
	for {
		select {
		case block, ok := <-vb.ch:
			if !ok {
				vb.ch = nil
				isBreak = true
				break
			}

			size += int64(len(block))
			rs := toVerifyResult(block)
			result.Results = append(result.Results, rs)
		case <-t.C:
			isBreak = true
		}

		if isBreak {
			break
		}

	}

	// nanosecond
	unit := time.Duration(1000000000)
	duration := time.Now().Sub(now)
	if duration < time.Duration(req.Duration)*unit {
		duration = time.Duration(req.Duration) * unit
	}

	result.Bandwidth = float64(size) / float64(duration) * float64(unit)
	result.CostTime = int(duration / 1000000)

	r := rand.New(rand.NewSource(req.Seed))
	results := make([]api.VerifyResult, 0, len(result.Results))

	for _, rs := range result.Results {
		random := r.Intn(len(req.FIDs))
		rs.Fid = req.FIDs[random]
		results = append(results, rs)
	}

	result.Results = results

	log.Infof("verify %s %d block, bandwidth:%f, cost time:%d, IsTimeout:%v, duration:%d, size:%d", result.DeviceID, len(result.Results), result.Bandwidth, result.CostTime, result.IsTimeout, req.Duration, size)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sendVerifyResult(ctx, candidate, result)
}

func verify(req api.ReqVerify, candidate CandidateAPI) {
	result := api.VerifyResults{RoundID: req.RoundID}
	result.Results = make([]api.VerifyResult, 0)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if len(req.FIDs) == 0 {
		sendVerifyResult(ctx, candidate, result)
		log.Errorf("len(req.FIDs) == 0 ")
		return
	}

	edgeAPI, closer, err := client.NewEdge(ctx, req.EdgeURL, nil)
	if err != nil {
		result.IsTimeout = true
		sendVerifyResult(ctx, candidate, result)
		log.Errorf("verify NewEdge err : %v", err)
		return
	}
	defer closer()

	info, err := edgeAPI.DeviceInfo(ctx)
	if err != nil {
		result.IsTimeout = true
		sendVerifyResult(ctx, candidate, result)
		log.Errorf("verify get device info err : %v", err)
		return
	}

	result.DeviceID = info.DeviceId

	vb, ok := verifyMap[info.DeviceId]
	if ok {
		log.Errorf("Aready doing verify edge node, deviceID:%s, not need to repeat to do", info.DeviceId)
		return
	}

	vb = &verifyBlock{conn: nil, ch: make(chan []byte)}
	verifyMap[info.DeviceId] = vb
	// defer verifyComplete(info.DeviceId)

	// var size = int64(0)
	go waitBlock(vb, req, candidate, result)

	wctx, cancel := context.WithTimeout(context.Background(), (time.Duration(req.Duration))*time.Second)
	defer cancel()

	err = edgeAPI.DoVerify(wctx, req, candidate.tcpSrvAddr)
	if err != nil {
		result.IsTimeout = true
		sendVerifyResult(ctx, candidate, result)
		log.Errorf("verify, edge DoVerify err : %v", err)
		return
	}

	// nanosecond
	// unit := time.Duration(1000000000)
	// duration := time.Now().Sub(now)
	// if duration < time.Duration(req.Duration)*unit {
	// 	duration = time.Duration(req.Duration) * unit
	// }

	// result.Bandwidth = float64(size) / float64(duration) * float64(unit)
	// result.CostTime = int(duration / 1000000)

	// r := rand.New(rand.NewSource(req.Seed))
	// results := make([]api.VerifyResult, 0, len(result.Results))

	// for _, rs := range result.Results {
	// 	random := r.Intn(len(req.FIDs))
	// 	rs.Fid = req.FIDs[random]
	// 	results = append(results, rs)
	// }

	// result.Results = results

	// log.Infof("verify %s %d block, bandwidth:%f, cost time:%d, IsTimeout:%v, duration:%d, size:%d", result.DeviceID, len(result.Results), result.Bandwidth, result.CostTime, result.IsTimeout, req.Duration, size)
	// sendVerifyResult(ctx, candidate, result)
}
