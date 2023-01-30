package downloader

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	blocks "github.com/ipfs/go-block-format"
	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/api/client"
	"github.com/linguohua/titan/node/carfile/carfilestore"
	"github.com/linguohua/titan/node/helper"
)

// type CandidateAPI struct {
// 	deviceID   string
// 	downSrvURL string
// 	token      string
// }

type candidate struct {
	carfileStore *carfilestore.CarfileStore
}

func NewCandidate(carfileStore *carfilestore.CarfileStore) *candidate {
	return &candidate{carfileStore: carfileStore}
}

func (candidate *candidate) DownloadBlocks(cids []string, downloadSource []*api.DowloadSource) ([]blocks.Block, error) {
	return getBlocksFromCandidate(cids, downloadSource)
}

func getBlockFromCandidateWithApi(candidate api.Candidate, cidStr string) (blocks.Block, error) {
	ctx, cancel := context.WithTimeout(context.Background(), helper.BlockDownloadTimeout*time.Second)
	defer cancel()

	data, err := candidate.LoadBlock(ctx, cidStr)
	if err != nil {
		return nil, err
	}

	return newBlock(cidStr, data)
}

func getBlocksFromCandidate(cids []string, sources []*api.DowloadSource) ([]blocks.Block, error) {
	// startTime := time.Now()
	blks := make([]blocks.Block, 0, len(cids))
	candidates := make(map[string]api.Candidate)
	blksLock := &sync.Mutex{}

	var wg sync.WaitGroup

	for index, cid := range cids {
		cidStr := cid
		i := index % len(sources)
		downloadSource := sources[i]

		candidate, err := getCandidateAPI(downloadSource.CandidateURL, downloadSource.CandidateToken, candidates)
		if err != nil {
			log.Errorf("loadBlocksFromCandidate getCandidateAPI error:%s", err.Error())
			continue
		}

		wg.Add(1)

		go func() {
			defer wg.Done()

			b, err := getBlockFromCandidateWithApi(candidate, cidStr)
			if err != nil {
				log.Errorf("getBlocksFromCandidateWithApi error:%s", err.Error())
				return
			}
			blksLock.Lock()
			blks = append(blks, b)
			blksLock.Unlock()
		}()
	}
	wg.Wait()

	// log.Infof("getBlocksFromCandidateWithApi block len:%d, duration:%dns", len(blks), time.Since(startTime))
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
	candicateAPI, _, err := client.NewCandicate(ctx, url, headers)
	if err != nil {
		log.Errorf("CandidateNodeConnect NewCandicate err:%s,url:%s", err.Error(), url)
		return nil, err
	}

	return candicateAPI, nil
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
