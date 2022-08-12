package candidate

import (
	"context"
	"fmt"
	"sync"
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

func NewLocalCandidateNode(ctx context.Context, ds datastore.Batching, scheduler api.Scheduler, blockStore stores.BlockStore, device device.DeviceAPI) api.Candidate {
	a := edge.NewLocalEdgeNode(ctx, ds, scheduler, blockStore, device)
	edgeAPI := a.(edge.EdgeAPI)
	return CandidateAPI{EdgeAPI: edgeAPI}
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
}

func (candidate CandidateAPI) WaitQuiet(ctx context.Context) error {
	log.Info("WaitQuiet")
	return nil
}

func (candidate CandidateAPI) VerifyData(ctx context.Context, req []api.ReqVarify) ([]api.VarifyResult, error) {
	log.Info("VerifyData")

	results := make([]*api.VarifyResult, 0, len(req))
	wg := &sync.WaitGroup{}

	for _, varify := range req {
		result := &api.VarifyResult{}
		results = append(results, result)

		wg.Add(1)
		go verify(ctx, wg, varify.Fid, varify.URL, result)
	}
	wg.Wait()

	res := make([]api.VarifyResult, 0, len(req))
	for _, result := range results {
		res = append(res, *result)
	}

	return res, nil
}

func verify(ctx context.Context, wg *sync.WaitGroup, fid, url string, result *api.VarifyResult) {
	defer wg.Done()

	result.Fid = fid

	edgeAPI, closer, err := client.NewEdge(ctx, url, nil)
	if err != nil {
		result.IsTimeout = true
		log.Errorf("VerifyData NewEdge err : %v", err)
		return
	}
	defer closer()

	now := time.Now().UnixMilli()
	data, err := edgeAPI.LoadDataByVerifier(ctx, fid)
	if err != nil {
		result.IsTimeout = true
		log.Errorf("VerifyData LoadDataByVerifier err : %v", err)
		return
	}

	result.CostTime = int(time.Now().UnixMilli() - now)

	cid, err := cidFromData(data)
	if err != nil {
		log.Errorf("VerifyData cidFromData err : %v", err)
		return
	}

	result.Cid = cid

	if result.CostTime > 0 {
		result.Bandwidth = float64(len(data)/result.CostTime) * 1000
	}

	info, err := edgeAPI.DeviceInfo(ctx)
	if err != nil {
		log.Errorf("VerifyData get device info err : %v", err)
		return
	}

	result.DeviceID = info.DeviceId
	log.Infof("fid:%s, cid:%s, deviceID:%s", result.Fid, result.Cid, result.DeviceID)
}
