package edge

import (
	"context"

	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/api/client"
)

var (
	candidateApiMap = make(map[string]api.Candidate)
	reqCacheDataCh  = make(chan api.ReqCacheData, 1000)
)

func startLoadBlockFromCandidate(ctx context.Context, edge EdgeAPI) {
	for {
		req := <-reqCacheDataCh

		loadBlocksFromCandidate(edge, req)
	}
}

func getCandidateAPI(candidateURL string) (api.Candidate, error) {
	candidateAPI, ok := candidateApiMap[candidateURL]
	if ok {
		return candidateAPI, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	candidateAPI, _, err := client.NewCandicate(ctx, candidateURL, nil)
	if err != nil {
		log.Errorf("getCandidateAPI, NewCandicate err:%v", err)
		return nil, err
	}

	candidateApiMap[candidateURL] = candidateAPI

	return candidateAPI, nil
}

func failAll(ctx context.Context, edge EdgeAPI, req api.ReqCacheData) {
	for _, cid := range req.Cids {
		cacheResult(ctx, edge, cid, false)
		log.Infof("loadBlocksFromCandidate fail, cid:%s", cid)
	}
}

func loadBlocksFromCandidate(edge EdgeAPI, req api.ReqCacheData) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	api, err := getCandidateAPI(req.CandidateURL)
	if err != nil {
		failAll(ctx, edge, req)
		log.Errorf("getCandidateAPI error:%v", err)
		return
	}

	failCids := make([]string, 0)
	for _, cid := range req.Cids {
		data, err := api.LoadData(ctx, cid)
		if err == nil {
			err = edge.blockStore.Put(cid, data)
		} else {
			failCids = append(failCids, cid)
		}

		cacheResult(ctx, edge, cid, err == nil)

		log.Infof("loadBlocksFromCandidate, cid:%s,err:%v", cid, err)
	}

	// delete api if all fail, make it connect next time
	if len(failCids) == len(req.Cids) {
		delete(candidateApiMap, req.CandidateURL)
	}
}

func addReq2Queue(edge EdgeAPI, req api.ReqCacheData) error {
	reqCacheDataCh <- req
	return nil
}
