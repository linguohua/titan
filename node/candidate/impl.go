package candidate

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/api/client"
	"github.com/linguohua/titan/stores"

	"github.com/linguohua/titan/node/device"
	"github.com/linguohua/titan/node/edge"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	logging "github.com/ipfs/go-log/v2"
	mh "github.com/multiformats/go-multihash"
)

var log = logging.Logger("candidate")

type verifyBlock struct {
	deviceID string
	data     []byte
}

var verifyChannelMap = make(map[string]chan verifyBlock)

func NewLocalCandidateNode(ctx context.Context, ds datastore.Batching, scheduler api.Scheduler, blockStore stores.BlockStore, device device.DeviceAPI, url string) api.Candidate {
	a := edge.NewLocalEdgeNode(ctx, ds, scheduler, blockStore, device, true)
	edgeAPI := a.(edge.EdgeAPI)
	return CandidateAPI{EdgeAPI: edgeAPI, url: url}
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
	url string
}

func (candidate CandidateAPI) WaitQuiet(ctx context.Context) error {
	log.Info("WaitQuiet")
	return nil
}

// edge node send block to candidate
func (candidate CandidateAPI) SendBlock(ctx context.Context, block []byte, deviceID string) error {
	log.Infof("SendBlock, len:%d", len(block))
	if deviceID == "" {
		return fmt.Errorf("deviceID is empty")
	}

	ch, ok := verifyChannelMap[deviceID]
	if !ok {
		return fmt.Errorf("Candidate no wait for verify block")
	}

	result := verifyBlock{deviceID: deviceID, data: block}
	ch <- result
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

func waitBlock(ctx context.Context, c chan verifyBlock, result *api.VerifyResults, size *int64) {
	for {
		select {
		case vb := <-c:
			*size += int64(len(vb.data))
			rs := toVerifyResult(vb.data)
			result.Results = append(result.Results, rs)
		case <-ctx.Done():
			log.Infof("Exit wait block")
			return
		}

	}
}

func verifyComplete(deviceID string) {
	delete(verifyChannelMap, deviceID)
}

func verify(req api.ReqVerify, candidate CandidateAPI) {
	result := api.VerifyResults{RoundID: req.RoundID}
	result.Results = make([]api.VerifyResult, 0)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

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

	ch, ok := verifyChannelMap[info.DeviceId]
	if ok {
		log.Errorf("Aready doing verify edge node, deviceID:%s, not need to repeat to do", info.DeviceId)
		return
	}

	ch = make(chan verifyBlock)
	verifyChannelMap[info.DeviceId] = ch
	defer verifyComplete(info.DeviceId)

	var size = int64(0)
	go waitBlock(ctx, ch, &result, &size)

	now := time.Now()
	err = edgeAPI.DoVerify(ctx, req, candidate.url)
	if err != nil {
		result.IsTimeout = true
		sendVerifyResult(ctx, candidate, result)
		log.Errorf("verify, edge DoVerify err : %v", err)
		return
	}

	duration := time.Now().Sub(now)
	if duration > 0 {
		result.Bandwidth = float64(size) / float64(duration) * 1000000000
		result.CostTime = int(duration / 1000000)
	}

	r := rand.New(rand.NewSource(req.Seed))
	results := make([]api.VerifyResult, 0, len(result.Results))
	for _, rs := range result.Results {
		fid := req.MaxRange
		if req.MaxRange > 1 {
			fid = r.Intn(req.MaxRange-1) + 1
		}

		rs.Fid = fmt.Sprintf("%d", fid)
		results = append(results, rs)
	}

	result.Results = results

	// for _, rs := range result.Results {
	// 	log.Infof("result fid:%s, cid:%s", rs.Fid, rs.Cid)
	// }

	log.Infof("verify %s %d block, bandwidth:%f, cost time:%d, IsTimeout:%v, duration:%d, size:%d", result.DeviceID, len(result.Results), result.Bandwidth, result.CostTime, result.IsTimeout, req.Duration, size)
	sendVerifyResult(ctx, candidate, result)
}
