package fetcher

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/linguohua/titan/api/types"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-libipfs/blocks"
)

type candidate struct {
	retryCount int
	httpClient *http.Client
}

func NewCandidate(timeout, retryCount int) *candidate {
	t := http.DefaultTransport.(*http.Transport).Clone()
	t.MaxIdleConns = 10
	t.IdleConnTimeout = 120 * time.Second

	httpClient := &http.Client{
		Timeout:   time.Duration(timeout) * time.Second,
		Transport: t,
	}

	return &candidate{retryCount: retryCount, httpClient: httpClient}
}

func (candidate *candidate) Fetch(ctx context.Context, cids []string, dss []*types.DownloadSource) ([]blocks.Block, error) {
	return candidate.getBlocks(cids, dss)
}

func (candidate *candidate) getBlock(ds *types.DownloadSource, cidStr string) (blocks.Block, error) {
	buf, err := encode(ds.Credentials)
	if err != nil {
		return nil, err
	}
	url := fmt.Sprintf("%s/ipfs/%s?format=raw", ds.CandidateURL, cidStr)
	url = strings.Replace(url, "https", "http", 1)
	log.Debugf("url:%s", url)
	log.Debugf("Credentials:%#v", ds.Credentials)

	req, err := http.NewRequest(http.MethodGet, url, buf)
	if err != nil {
		return nil, err
	}

	resp, err := candidate.httpClient.Do(req)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("status code %d", resp.StatusCode)
	}

	data, err := ioutil.ReadAll(resp.Body)
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
	// candidates := make(map[string]api.Candidate)
	blksLock := &sync.Mutex{}

	var wg sync.WaitGroup

	for index, cid := range cids {
		cidStr := cid
		i := index % len(dss)
		ds := dss[i]

		wg.Add(1)

		go func() {
			defer wg.Done()

			for i := 0; i < candidate.retryCount; i++ {
				b, err := candidate.getBlock(ds, cidStr)
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

func encode(gwCredentials *types.GatewayCredentials) (*bytes.Buffer, error) {
	var buffer bytes.Buffer
	enc := gob.NewEncoder(&buffer)
	err := enc.Encode(gwCredentials)
	if err != nil {
		return nil, err
	}

	return &buffer, nil
}
