package block

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/linguohua/titan/api/client"
)

type Candidate struct {
	deviceID   string
	downSrvURL string
	token      string
}

func (candidate *Candidate) loadBlocks(block *Block, req []*delayReq) {
	loadBlocksFromCandidate(block, req)
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

func getCandidate(candidateURL string) (*Candidate, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	api, close, err := client.NewCandicate(ctx, candidateURL, nil)
	if err != nil {
		log.Errorf("getCandidateAPI, NewCandicate err:%v", err)
		return nil, err
	}
	defer close()

	info, err := api.DeviceInfo(ctx)
	if err != nil {
		log.Errorf("getCandidateAPI, NewCandicate err:%v", err)
		return nil, err
	}

	download, err := api.GetDownloadInfo(ctx)
	if err != nil {
		log.Errorf("getCandidateAPI, NewCandicate err:%v", err)
		return nil, err
	}

	candidate := &Candidate{deviceID: info.DeviceId, downSrvURL: download.URL, token: download.Token}
	return candidate, nil
}

func getCandidateWithMap(candidateMap map[string]*Candidate, candidateURL string) (*Candidate, error) {
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

func loadBlocksFromCandidate(block *Block, reqs []*delayReq) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	candidateMap := make(map[string]*Candidate)
	for _, req := range reqs {
		candidate, err := getCandidateWithMap(candidateMap, req.candidateURL)
		if err != nil {
			log.Errorf("getCandidateWithMap error:%v", err)
			block.cacheResult(ctx, req.cid, "", err)
			continue
		}

		url := fmt.Sprintf("%s?cid=%s", candidate.downSrvURL, req.cid)

		data, err := getBlockFromCandidate(url, candidate.token)
		if err == nil {
			err = block.blockStore.Put(req.cid, data)
		}

		block.cacheResult(ctx, req.cid, candidate.deviceID, err)
		log.Infof("loadBlocksFromCandidate, cid:%s,err:%s", req.cid, err.Error())
	}
}
