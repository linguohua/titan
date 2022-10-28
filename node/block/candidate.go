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
	reqs = block.filterAvailableReq(reqs)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	candidateMap := make(map[string]*Candidate)
	for _, req := range reqs {
		candidate, err := getCandidateWithMap(candidateMap, req.candidateURL)
		if err != nil {
			log.Errorf("getCandidateWithMap error:%v", err)
			block.cacheResultWithError(ctx, req.blockInfo.Cid, err)
			continue
		}

		url := fmt.Sprintf("%s?cid=%s", candidate.downSrvURL, req.blockInfo.Cid)

		data, err := getBlockFromCandidate(url, candidate.token)
		if err != nil {
			log.Errorf("loadBlocksFromCandidate get block from candidate error:%s", err.Error())
			block.cacheResultWithError(ctx, req.blockInfo.Cid, err)
			continue
		}

		err = block.saveBlock(ctx, data, req.blockInfo.Cid, req.blockInfo.Fid)
		if err != nil {
			log.Errorf("loadBlocksFromCandidate save block error:%s", err.Error())
			block.cacheResultWithError(ctx, req.blockInfo.Cid, err)
			continue
		}

		links, err := getLinks(block, data, req.blockInfo.Cid)
		if err != nil {
			log.Errorf("loadBlocksFromCandidate resolveLinks error:%s", err.Error())
			block.cacheResultWithError(ctx, req.blockInfo.Cid, err)
			continue
		}

		linksSize := uint64(0)
		cids := make([]string, 0, len(links))
		for _, link := range links {
			cids = append(cids, link.Cid.String())
			linksSize += link.Size
		}

		bInfo := blockStat{cid: req.blockInfo.Cid, fid: req.blockInfo.Fid, links: cids, blockSize: len(data), linksSize: linksSize, carFileCid: req.carFileCid, CacheID: req.CacheID}
		block.cacheResult(ctx, candidate.deviceID, nil, bInfo)

		log.Infof("loadBlocksFromCandidate, cid:%s,err:%v", req.blockInfo.Cid, err)
	}
}
