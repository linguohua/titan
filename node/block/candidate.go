package block

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/api/client"
	"github.com/linguohua/titan/node/helper"
)

type Candidate struct {
	deviceID   string
	downSrvURL string
	token      string
}

func (candidate *Candidate) loadBlocks(block *Block, req []*delayReq) {
	loadBlocksFromCandidate(block, req)
}

func (candidate *Candidate) syncData(block *Block, reqs []*DataSyncReq) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	candidates := make(map[string]api.Candidate)

	for _, req := range reqs {
		candidate, err := getCandidateAPI(req.DownloadURL, req.DownloadToken, candidates)
		if err != nil {
			log.Errorf("syncData getCandidateAPI error:%s", err.Error())
			continue
		}

		data, err := candidate.LoadBlock(ctx, req.Cid)
		if err != nil {
			log.Errorf("syncData LoadBlock error:%s", err.Error())
			continue
		}

		err = block.saveBlock(ctx, data, req.Cid, fmt.Sprintf("%d", req.Fid))
		if err != nil {
			log.Errorf("syncData save block error:%s", err.Error())
			continue
		}
	}
	return nil
}

func getBlockFromCandidate(url string, tk string) ([]byte, error) {
	client := &http.Client{Timeout: helper.BlockDownloadTimeout * time.Second}
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

func newCandidateAPI(url string, tk string) (api.Candidate, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	headers := http.Header{}
	headers.Add("Authorization", "Bearer "+string(tk))

	// Connect to node
	candicateAPI, _, err := client.NewCandicate(ctx, url, headers)
	if err != nil {
		log.Errorf("CandidateNodeConnect NewCandicate err:%s,url:%s", err.Error(), url)
		return nil, err
	}

	return candicateAPI, nil
}

func getCandidateAPI(url string, tk string, candidates map[string]api.Candidate) (api.Candidate, error) {
	candidate, ok := candidates[url]
	if !ok {
		var err error
		candidate, err = newCandidateAPI(url, tk)
		if err != nil {
			return nil, err
		}
		candidates[url] = candidate
	}
	return candidate, nil
}

func loadBlocksFromCandidate(block *Block, reqs []*delayReq) {
	reqs = block.filterAvailableReq(reqs)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	candidates := make(map[string]api.Candidate)
	for _, req := range reqs {
		if len(req.downloadURL) == 0 {
			log.Errorf("loadBlocksFromCandidate req downloadURL is nil")
			block.cacheResultWithError(ctx, blockStat{cid: req.blockInfo.Cid, carFileHash: req.carFileHash, CacheID: req.CacheID}, fmt.Errorf("candidate download url is empty"))
			continue
		}

		// url := fmt.Sprintf("%s?cid=%s", req.downloadURL, req.blockInfo.Cid)
		// data, err := getBlockFromCandidate(url, req.downloadToken)
		candidate, err := getCandidateAPI(req.downloadURL, req.downloadToken, candidates)
		if err != nil {
			log.Errorf("loadBlocksFromCandidate getCandidateAPI error:%s", err.Error())
			block.cacheResultWithError(ctx, blockStat{cid: req.blockInfo.Cid, carFileHash: req.carFileHash, CacheID: req.CacheID}, err)
			continue
		}

		data, err := candidate.LoadBlock(ctx, req.blockInfo.Cid)
		if err != nil {
			log.Errorf("loadBlocksFromCandidate LoadBlock error:%s", err.Error())
			block.cacheResultWithError(ctx, blockStat{cid: req.blockInfo.Cid, carFileHash: req.carFileHash, CacheID: req.CacheID}, err)
			continue
		}

		err = block.saveBlock(ctx, data, req.blockInfo.Cid, fmt.Sprintf("%d", req.blockInfo.Fid))
		if err != nil {
			log.Errorf("loadBlocksFromCandidate save block error:%s", err.Error())
			block.cacheResultWithError(ctx, blockStat{cid: req.blockInfo.Cid, carFileHash: req.carFileHash, CacheID: req.CacheID}, err)
			continue
		}

		links, err := getLinks(block, data, req.blockInfo.Cid)
		if err != nil {
			log.Errorf("loadBlocksFromCandidate resolveLinks error:%s", err.Error())
			block.cacheResultWithError(ctx, blockStat{cid: req.blockInfo.Cid, carFileHash: req.carFileHash, CacheID: req.CacheID}, err)
			continue
		}

		linksSize := uint64(0)
		cids := make([]string, 0, len(links))
		for _, link := range links {
			cids = append(cids, link.Cid.String())
			linksSize += link.Size
		}

		bInfo := blockStat{cid: req.blockInfo.Cid, links: cids, blockSize: len(data), linksSize: linksSize, carFileHash: req.carFileHash, CacheID: req.CacheID}
		block.cacheResult(ctx, nil, bInfo)

		log.Infof("loadBlocksFromCandidate, cid:%s,err:%v", req.blockInfo.Cid, err)
	}
}

func syncDataFromCandidate(block *Block, reqs []*DataSyncReq) error {
	return nil
}
