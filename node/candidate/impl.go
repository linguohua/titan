package candidate

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/api/client"
	"github.com/linguohua/titan/stores"

	"github.com/linguohua/titan/node/common"
	"github.com/linguohua/titan/node/device"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	logging "github.com/ipfs/go-log/v2"
	mh "github.com/multiformats/go-multihash"
)

var log = logging.Logger("main")

func NewLocalCandidateNode(ds datastore.Batching, scheduler api.Scheduler, blockStore stores.BlockStore, deviceID, publicIP string) api.Candidate {
	return CandidateAPI{ds: ds, scheduler: scheduler, blockStore: blockStore, DeviceAPI: device.DeviceAPI{DeviceID: deviceID, PublicIP: publicIP}}
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
	common.CommonAPI
	device.DeviceAPI
	ds         datastore.Batching
	scheduler  api.Scheduler
	blockStore stores.BlockStore
}

func (candidate CandidateAPI) WaitQuiet(ctx context.Context) error {
	return nil
}

func (candidate CandidateAPI) VerifyData(ctx context.Context, req []api.ReqVarify) ([]api.VarifyResult, error) {
	results := make([]api.VarifyResult, 0, len(req))
	wg := sync.WaitGroup{}

	for _, varify := range req {
		result := &api.VarifyResult{}
		results = append(results, *result)

		wg.Add(1)
		go verify(ctx, wg, varify.Fid, varify.URL, result)
	}
	wg.Wait()

	return results, nil
}

func verify(ctx context.Context, wg sync.WaitGroup, fid, url string, result *api.VarifyResult) {
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

	cid, err := cidFromData(data)
	if err != nil {
		log.Errorf("VerifyData LoadDataByVerifier err : %v", err)
		return
	}

	result.Cid = cid
	result.CostTime = int(time.Now().UnixMilli() - now)

	if result.CostTime > 0 {
		result.Bandwidth = float64(len(data)/result.CostTime) * 1000
	}

	return
}
