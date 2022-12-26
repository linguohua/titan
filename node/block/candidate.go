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

			data, err := getBlockFromCandidateWithApi(candidate, cid)
			if err != nil {
				log.Errorf("syncData LoadBlock error:%s", err.Error())
				continue
			}

			err = block.saveBlock(ctx, data, cid, fmt.Sprintf("%d", blockFIDMap[cid]))
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

func getBlockFromCandidateWithApi(candidate api.Candidate, cid string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), helper.BlockDownloadTimeout*time.Second)
	defer cancel()

	data, err := candidate.LoadBlock(ctx, cid)
	if err != nil {
		return nil, err
	}

	return data, nil
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
	candidates := make(map[string]api.Candidate)

	for _, req := range reqs {
		if len(req.downloadURL) == 0 {
			log.Errorf("loadBlocksFromCandidate req downloadURL is nil")
			block.cacheResultWithError(blockStat{cid: req.blockInfo.Cid, carFileHash: req.carFileHash, CacheID: req.CacheID}, fmt.Errorf("candidate download url is empty"))
			continue
		}

		// url := fmt.Sprintf("%s?cid=%s", req.downloadURL, req.blockInfo.Cid)
		// data, err := getBlockFromCandidate(url, req.downloadToken)
		candidate, err := getCandidateAPI(req.downloadURL, req.downloadToken, candidates)
		if err != nil {
			log.Errorf("loadBlocksFromCandidate getCandidateAPI error:%s", err.Error())
			block.cacheResultWithError(blockStat{cid: req.blockInfo.Cid, carFileHash: req.carFileHash, CacheID: req.CacheID}, err)
			continue
		}

		data, err := getBlockFromCandidateWithApi(candidate, req.blockInfo.Cid)
		if err != nil {
			log.Errorf("loadBlocksFromCandidate LoadBlock error:%s", err.Error())
			block.cacheResultWithError(blockStat{cid: req.blockInfo.Cid, carFileHash: req.carFileHash, CacheID: req.CacheID}, err)
			continue
		}

		err = block.saveBlock(context.Background(), data, req.blockInfo.Cid, fmt.Sprintf("%d", req.blockInfo.Fid))
		if err != nil {
			log.Errorf("loadBlocksFromCandidate save block error:%s", err.Error())
			block.cacheResultWithError(blockStat{cid: req.blockInfo.Cid, carFileHash: req.carFileHash, CacheID: req.CacheID}, err)
			continue
		}

		links, err := getLinks(block, data, req.blockInfo.Cid)
		if err != nil {
			log.Errorf("loadBlocksFromCandidate resolveLinks error:%s", err.Error())
			block.cacheResultWithError(blockStat{cid: req.blockInfo.Cid, carFileHash: req.carFileHash, CacheID: req.CacheID}, err)
			continue
		}

		linksSize := uint64(0)
		cids := make([]string, 0, len(links))
		for _, link := range links {
			cids = append(cids, link.Cid.String())
			linksSize += link.Size
		}

		bInfo := blockStat{cid: req.blockInfo.Cid, links: cids, blockSize: len(data), linksSize: linksSize, carFileHash: req.carFileHash, CacheID: req.CacheID}
		block.cacheResult(bInfo, nil)

		log.Infof("loadBlocksFromCandidate, cid:%s,err:%v", req.blockInfo.Cid, err)
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
