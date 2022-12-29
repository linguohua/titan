package block

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"sync"
	"time"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
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

func (candidate *Candidate) syncData(block *Block, reqs map[int]string) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if len(reqs) == 0 {
		return nil
	}

	blockFIDMap := make(map[string]int)
	cids := make([]string, 0, len(reqs))
	for fid, cid := range reqs {
		cids = append(cids, cid)
		blockFIDMap[cid] = fid
	}

	candidates := make(map[string]api.Candidate)
	groups := groupCids(cids)

	for _, group := range groups {
		infos, err := getCandidateDownloadInfoWithBlocks(block.scheduler, group)
		if err != nil {
			log.Errorf("syncData GetCandidateDownloadInfo error:%s", err.Error())
			continue
		}

		for cid, info := range infos {
			candidate, err := getCandidateAPI(info.URL, info.Token, candidates)
			if err != nil {
				log.Errorf("syncData getCandidateAPI error:%s", err.Error())
				continue
			}

			blk, err := getBlockFromCandidateWithApi(candidate, cid)
			if err != nil {
				log.Errorf("syncData LoadBlock error:%s", err.Error())
				continue
			}

			err = block.saveBlock(ctx, blk.RawData(), cid, fmt.Sprintf("%d", blockFIDMap[cid]))
			if err != nil {
				log.Errorf("syncData save block error:%s", err.Error())
				continue
			}
		}

	}

	return nil
}

func getCandidateDownloadInfoWithBlocks(scheduler api.Scheduler, cids []string) (map[string]api.CandidateDownloadInfo, error) {
	ctx, cancel := context.WithTimeout(context.Background(), helper.SchedulerApiTimeout*time.Second)
	defer cancel()

	return scheduler.GetCandidateDownloadInfoWithBlocks(ctx, cids)
}

func getBlockFromCandidateWithHttp(url string, tk string) ([]byte, error) {
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

func getBlockFromCandidateWithApi(candidate api.Candidate, cidStr string) (blocks.Block, error) {
	ctx, cancel := context.WithTimeout(context.Background(), helper.BlockDownloadTimeout*time.Second)
	defer cancel()

	data, err := candidate.LoadBlock(ctx, cidStr)
	if err != nil {
		return nil, err
	}

	target, err := cid.Decode(cidStr)
	if err != nil {
		return nil, err
	}

	basicBlock, err := blocks.NewBlockWithCid(data, target)
	if err != nil {
		return nil, err
	}

	return basicBlock, nil
}

func getBlocksFromCandidateWithApi(reqs []*delayReq) ([]blocks.Block, error) {
	startTime := time.Now()
	blks := make([]blocks.Block, 0, len(reqs))
	candidates := make(map[string]api.Candidate)

	var wg sync.WaitGroup

	for _, req := range reqs {
		delayReq := req

		candidate, err := getCandidateAPI(delayReq.downloadURL, delayReq.downloadToken, candidates)
		if err != nil {
			log.Errorf("loadBlocksFromCandidate getCandidateAPI error:%s", err.Error())
			continue
		}

		wg.Add(1)

		go func() {
			defer wg.Done()

			b, err := getBlockFromCandidateWithApi(candidate, delayReq.blockInfo.Cid)
			if err != nil {
				log.Errorf("getBlocksFromCandidateWithApi error:%s", err.Error())
				return
			}
			blks = append(blks, b)
		}()
	}
	wg.Wait()

	log.Infof("getBlocksFromCandidateWithApi block len:%d, duration:%dns", len(blks), time.Since(startTime))
	return blks, nil
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

// func loadBlocksFromCandidate(block *Block, reqs []*delayReq) {
// 	reqs = block.filterAvailableReq(reqs)
// 	candidates := make(map[string]api.Candidate)

// 	for _, req := range reqs {
// 		if len(req.downloadURL) == 0 {
// 			log.Errorf("loadBlocksFromCandidate req downloadURL is nil")
// 			block.cacheResultWithError(blockStat{cid: req.blockInfo.Cid, carFileHash: req.carFileHash, CacheID: req.CacheID}, fmt.Errorf("candidate download url is empty"))
// 			continue
// 		}

// 		// url := fmt.Sprintf("%s?cid=%s", req.downloadURL, req.blockInfo.Cid)
// 		// data, err := getBlockFromCandidate(url, req.downloadToken)
// 		candidate, err := getCandidateAPI(req.downloadURL, req.downloadToken, candidates)
// 		if err != nil {
// 			log.Errorf("loadBlocksFromCandidate getCandidateAPI error:%s", err.Error())
// 			block.cacheResultWithError(blockStat{cid: req.blockInfo.Cid, carFileHash: req.carFileHash, CacheID: req.CacheID}, err)
// 			continue
// 		}

// 		data, err := getBlockFromCandidateWithApi(candidate, req.blockInfo.Cid)
// 		if err != nil {
// 			log.Errorf("loadBlocksFromCandidate LoadBlock error:%s", err.Error())
// 			block.cacheResultWithError(blockStat{cid: req.blockInfo.Cid, carFileHash: req.carFileHash, CacheID: req.CacheID}, err)
// 			continue
// 		}

// 		err = block.saveBlock(context.Background(), data, req.blockInfo.Cid, fmt.Sprintf("%d", req.blockInfo.Fid))
// 		if err != nil {
// 			log.Errorf("loadBlocksFromCandidate save block error:%s", err.Error())
// 			block.cacheResultWithError(blockStat{cid: req.blockInfo.Cid, carFileHash: req.carFileHash, CacheID: req.CacheID}, err)
// 			continue
// 		}

// 		links, err := getLinks(block, data, req.blockInfo.Cid)
// 		if err != nil {
// 			log.Errorf("loadBlocksFromCandidate resolveLinks error:%s", err.Error())
// 			block.cacheResultWithError(blockStat{cid: req.blockInfo.Cid, carFileHash: req.carFileHash, CacheID: req.CacheID}, err)
// 			continue
// 		}

// 		linksSize := uint64(0)
// 		cids := make([]string, 0, len(links))
// 		for _, link := range links {
// 			cids = append(cids, link.Cid.String())
// 			linksSize += link.Size
// 		}

// 		bInfo := blockStat{cid: req.blockInfo.Cid, links: cids, blockSize: len(data), linksSize: linksSize, carFileHash: req.carFileHash, CacheID: req.CacheID}
// 		block.cacheResult(bInfo, nil)

// 		log.Infof("loadBlocksFromCandidate, cid:%s,err:%v", req.blockInfo.Cid, err)
// 	}
// }

func loadBlocksFromCandidate(block *Block, reqs []*delayReq) {
	reqs = block.filterAvailableReq(reqs)
	if len(reqs) == 0 {
		log.Debug("loadBlocksFromCandidate, len(reqs) == 0")
		return
	}

	ctx := context.Background()

	cids := make([]string, 0, len(reqs))
	reqMap := make(map[string]*delayReq)
	for _, reqData := range reqs {
		cids = append(cids, reqData.blockInfo.Cid)
		reqMap[reqData.blockInfo.Cid] = reqData
	}

	blocks, err := getBlocksFromCandidateWithApi(reqs)
	if err != nil {
		log.Errorf("loadBlocksFromCandidate getBlocksFromCandidateWithApi err %v", err)
		return
	}

	for _, b := range blocks {
		cidStr := b.Cid().String()
		req, ok := reqMap[cidStr]
		if !ok {
			log.Errorf("loadBlocksFromIPFS cid %s not in map", cidStr)
			continue
		}

		err = block.saveBlock(ctx, b.RawData(), req.blockInfo.Cid, fmt.Sprintf("%d", req.blockInfo.Fid))
		if err != nil {
			log.Errorf("loadBlocksFromIPFS save block error:%s", err.Error())
			continue
		}

		// get block links
		links, err := block.resolveLinks(b)
		if err != nil {
			log.Errorf("loadBlocksFromIPFS resolveLinks error:%s", err.Error())
			continue
		}

		linksSize := uint64(0)
		cids := make([]string, 0, len(links))
		for _, link := range links {
			cids = append(cids, link.Cid.String())
			linksSize += link.Size
		}

		bStat := blockStat{cid: cidStr, links: cids, blockSize: len(b.RawData()), linksSize: linksSize, carFileHash: req.carFileHash, CacheID: req.CacheID}
		block.cacheResult(bStat, nil)

		log.Infof("cache data,cid:%s,err:%v", cidStr, err)

		delete(reqMap, cidStr)
	}

	err = fmt.Errorf("Request timeout")
	tryDelayReqs := make([]*delayReq, 0)
	for _, v := range reqMap {
		if v.count >= helper.BlockDownloadRetryNum {
			block.cacheResultWithError(blockStat{cid: v.blockInfo.Cid, carFileHash: v.carFileHash, CacheID: v.CacheID}, err)
			log.Infof("cache data faile, cid:%s, count:%d", v.blockInfo.Cid, v.count)
		} else {
			v.count++
			delayReq := v
			tryDelayReqs = append(tryDelayReqs, delayReq)
		}
	}

	if len(tryDelayReqs) > 0 {
		loadBlocksFromCandidate(block, tryDelayReqs)
	}
}

// do in batch
func groupCids(cids []string) [][]string {
	sizeOfGroup := 1000
	groups := make([][]string, 0)
	for i := 0; i < len(cids); i += sizeOfGroup {
		j := i + sizeOfGroup
		if j > len(cids) {
			j = len(cids)
		}

		group := cids[i:j]
		groups = append(groups, group)
	}

	return groups
}
