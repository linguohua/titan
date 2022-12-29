package block

import (
	"context"
	"fmt"
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

func (candidate *Candidate) loadBlocks(block *Block, reqs []*delayReq) ([]blocks.Block, error) {
	return getBlocksFromCandidate(reqs)
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

func getBlocksFromCandidate(reqs []*delayReq) ([]blocks.Block, error) {
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
