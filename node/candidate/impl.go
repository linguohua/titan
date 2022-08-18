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
	a := edge.NewLocalEdgeNode(ctx, ds, scheduler, blockStore, device)
	edgeAPI := a.(edge.EdgeAPI)
	return CandidateAPI{EdgeAPI: edgeAPI, url: url}
}

func cidFromData(data []byte) (string, error) {
	pref := cid.Prefix{
		Version:  1,
		Codec:    uint64(cid.DagProtobuf),
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
		go verify(ctx, reqVerify, candidate.url)
	}

	return nil
}

func sendVerifyResult(ctx context.Context, result api.VerifyResults) {

}

func toVerifyResult(data []byte) api.VerifyResult {
	result := api.VerifyResult{}
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

func verify(ctx context.Context, req api.ReqVerify, candidateURL string) {
	result := api.VerifyResults{}
	result.Results = make([]api.VerifyResult, 0)

	edgeAPI, closer, err := client.NewEdge(ctx, req.EdgeURL, nil)
	if err != nil {
		result.IsTimeout = true
		sendVerifyResult(ctx, result)
		log.Errorf("verify NewEdge err : %v", err)
		return
	}
	defer closer()

	info, err := edgeAPI.DeviceInfo(ctx)
	if err != nil {
		result.IsTimeout = true
		sendVerifyResult(ctx, result)
		log.Errorf("verify get device info err : %v", err)
		return
	}

	result.DeviceID = info.DeviceId

	ch, ok := verifyChannelMap[info.DeviceId]
	if ok {
		log.Errorf("Aready verify edge node, deviceID:%s", info.DeviceId)
		return
	}

	ch = make(chan verifyBlock)
	verifyChannelMap[info.DeviceId] = ch

	wctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var size = int64(0)
	go waitBlock(wctx, ch, &result, &size)

	now := time.Now()
	err = edgeAPI.DoVerify(ctx, req, candidateURL)
	if err != nil {
		result.IsTimeout = true
		sendVerifyResult(ctx, result)
		log.Errorf("verify LoadDataByVerifier err : %v", err)
		return
	}

	duration := time.Now().Sub(now)
	if duration > 0 {
		result.Bandwidth = float64(size/int64(duration)) * 1000000000
		result.CostTime = int(duration / 100000)
	}

	r := rand.New(rand.NewSource(req.Seed))
	for _, rs := range result.Results {
		rs.Fid = fmt.Sprintf("%d", r.Intn(req.MaxRange))
	}

	log.Infof("verify %s %d block, bandwidth:%f, cost time:%d, IsTimeout:%v", result.DeviceID, result.Bandwidth, result.CostTime, result.IsTimeout)
	sendVerifyResult(ctx, result)
}
