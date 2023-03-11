package fetcher

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/linguohua/titan/api/types"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/api/client"
)

type candidate struct {
	timeout    int
	retryCount int
}

func NewCandidate(timeout, retryCount int) *candidate {
	return &candidate{timeout: timeout, retryCount: retryCount}
}

func (candidate *candidate) Fetch(ctx context.Context, cids []string, dss []*types.DownloadSource) ([]blocks.Block, error) {
	return candidate.getBlocks(cids, dss)
}

func (candidate *candidate) getBlock(candidateAPI api.Candidate, cidStr string) (blocks.Block, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(candidate.timeout)*time.Second)
	defer cancel()

	data, err := candidateAPI.GetBlock(ctx, cidStr)
	if err != nil {
		return nil, err
	}

	c, err := cid.Decode(cidStr)
	if err != nil {
		return nil, err
	}

	basicBlock, err := blocks.NewBlockWithCid(data, c)
	if err != nil {
		return nil, err
	}

	return basicBlock, nil
}

func (candidate *candidate) getBlocks(cids []string, dss []*types.DownloadSource) ([]blocks.Block, error) {
	if len(dss) == 0 {
		return nil, fmt.Errorf("download srouce can not empty")
	}

	blks := make([]blocks.Block, 0, len(cids))
	candidates := make(map[string]api.Candidate)
	blksLock := &sync.Mutex{}

	var wg sync.WaitGroup

	for index, cid := range cids {
		cidStr := cid
		i := index % len(dss)
		ds := dss[i]

		candidateAPI, err := getCandidateAPI(ds.CandidateURL, ds.CandidateToken, candidates)
		if err != nil {
			log.Errorf("loadBlocksFromCandidate getCandidateAPI error:%s", err.Error())
			continue
		}

		wg.Add(1)

		go func() {
			defer wg.Done()

			for i := 0; i < candidate.retryCount; i++ {
				b, err := candidate.getBlock(candidateAPI, cidStr)
				if err != nil {
					log.Errorf("getBlock error:%s, cid:%s", err.Error(), cidStr)
					continue
				}
				blksLock.Lock()
				blks = append(blks, b)
				blksLock.Unlock()
				return
			}
		}()
	}
	wg.Wait()

	return blks, nil
}

func newCandidateAPI(url string, tk string) (api.Candidate, error) {
	if len(url) == 0 || len(tk) == 0 {
		return nil, fmt.Errorf("newCandidateAPI failed, url:%s, token:%s", url, tk)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	headers := http.Header{}
	headers.Add("Authorization", "Bearer "+string(tk))

	// Connect to node
	candidateAPI, _, err := client.NewCandicate(ctx, url, headers)
	if err != nil {
		log.Errorf("CandidateNodeConnect NewCandicate err:%s,url:%s", err.Error(), url)
		return nil, err
	}

	return candidateAPI, nil
}

func getCandidateAPI(url string, tk string, candidates map[string]api.Candidate) (api.Candidate, error) {
	candidate, exist := candidates[url]
	if !exist {
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
