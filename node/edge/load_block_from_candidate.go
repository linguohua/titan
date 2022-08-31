package edge

import (
	"context"

	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/api/client"
)

type candidateApi struct {
	api      api.Candidate
	deviceID string
}

func getCandidateAPI(candidateURL string) (*candidateApi, error) {
	candidate, ok := candidateApiMap[candidateURL]
	if ok {
		return candidate, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	api, _, err := client.NewCandicate(ctx, candidateURL, nil)
	if err != nil {
		log.Errorf("getCandidateAPI, NewCandicate err:%v", err)
		return nil, err
	}

	info, err := api.DeviceInfo(ctx)
	if err != nil {
		log.Errorf("getCandidateAPI, NewCandicate err:%v", err)
		return nil, err
	}

	candidate = &candidateApi{api: api, deviceID: info.DeviceId}

	candidateApiMap[candidateURL] = candidate

	return candidate, nil
}

func loadBlocksFromCandidate(edge *Edge, reqs []delayReq) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	reqMap := make(map[string][]delayReq)
	for _, req := range reqs {
		rqs, ok := reqMap[req.candidateURL]
		if !ok {
			rqs = make([]delayReq, 0)
		}
		rqs = append(rqs, req)
		reqMap[req.candidateURL] = rqs
	}

	failMap := make(map[string][]delayReq)
	for _, req := range reqs {
		candidate, err := getCandidateAPI(req.candidateURL)
		if err != nil {
			cacheResult(ctx, edge, req.cid, "", err)
			log.Errorf("getCandidateAPI error:%v", err)
			continue
		}

		data, err := candidate.api.LoadData(ctx, req.cid)
		if err == nil {
			err = edge.blockStore.Put(req.cid, data)
		} else {
			fails, ok := failMap[req.candidateURL]
			if !ok {
				fails = make([]delayReq, 0)
			}
			fails = append(fails, req)
			failMap[req.candidateURL] = fails
		}

		cacheResult(ctx, edge, req.cid, candidate.deviceID, err)

		log.Infof("loadBlocksFromCandidate, cid:%s,err:%v", req.cid, err)
	}

	// may be need to delete connection if all fail, make it connect next time
	for k, v := range failMap {
		if len(v) == len(reqMap[k]) {
			delete(candidateApiMap, k)
		}
	}
}
