package edge

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/api/client"
)

type candidate struct {
	api        api.Candidate
	deviceID   string
	downSrvURL string
	token      string
}

func getBlockFromCandidate(url string, tk string) ([]byte, error) {
	client := &http.Client{}
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Token", tk)
	req.Header.Set("App-Name", "edge")

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	return ioutil.ReadAll(resp.Body)
}

func getCandidate(candidateURL string) (*candidate, error) {
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

	tk, err := api.GenerateDownloadToken(ctx)
	if err != nil {
		log.Errorf("getCandidateAPI, NewCandicate err:%v", err)
		return nil, err
	}

	candidate := &candidate{api: api, deviceID: info.DeviceId, downSrvURL: info.DownloadSrvURL, token: tk}
	return candidate, nil
}

func getCandidateWithMap(candidateMap map[string]*candidate, candidateURL string) (*candidate, error) {
	var err error
	candidate, ok := candidateMap[candidateURL]
	if !ok {
		candidate, err = getCandidate(candidateURL)
		if err != nil {
			return nil, err
		}
		candidateMap[candidateURL] = candidate
	}
	return candidate, nil
}

func loadBlocksFromCandidate(edge *Edge, reqs []delayReq) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	candidateMap := make(map[string]*candidate)
	for _, req := range reqs {
		candidate, err := getCandidateWithMap(candidateMap, req.candidateURL)
		if err != nil {
			log.Errorf("getCandidateWithMap error:%v", err)
			cacheResult(ctx, edge, req.cid, "", err)
			continue
		}

		url := fmt.Sprintf("%s?cid=%s", candidate.downSrvURL, req.cid)

		data, err := getBlockFromCandidate(url, candidate.token)
		if err == nil {
			err = edge.blockStore.Put(req.cid, data)
		}

		cacheResult(ctx, edge, req.cid, candidate.deviceID, err)
		log.Infof("loadBlocksFromCandidate, cid:%s,err:%v", req.cid, err)
	}
}
