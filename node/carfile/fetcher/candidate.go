package fetcher

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"io/ioutil"
	"net/http"
	"sync"
	"time"

	"github.com/linguohua/titan/api/types"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-libipfs/blocks"
)

type Candidate struct {
	retryCount int
	httpClient *http.Client
}

func NewCandidate(timeout, retryCount int) *Candidate {
	t := http.DefaultTransport.(*http.Transport).Clone()
	t.MaxIdleConns = 10
	t.IdleConnTimeout = 120 * time.Second

	httpClient := &http.Client{
		Timeout:   time.Duration(timeout) * time.Second,
		Transport: t,
	}

	return &Candidate{retryCount: retryCount, httpClient: httpClient}
}

func (c *Candidate) Fetch(ctx context.Context, cids []string, dss []*types.DownloadSource) ([]blocks.Block, error) {
	return c.getBlocks(cids, dss)
}

func (c *Candidate) getBlock(ds *types.DownloadSource, cidStr string) (blocks.Block, error) {
	buf, err := encode(ds.Credentials)
	if err != nil {
		return nil, err
	}
	url := fmt.Sprintf("http://%s/ipfs/%s?format=raw", ds.CandidateAddr, cidStr)

	req, err := http.NewRequest(http.MethodGet, url, buf)
	if err != nil {
		return nil, err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close() //nolint:errcheck // ignore error

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("status code %d", resp.StatusCode)
	}

	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	cid, err := cid.Decode(cidStr)
	if err != nil {
		return nil, err
	}

	basicBlock, err := blocks.NewBlockWithCid(data, cid)
	if err != nil {
		return nil, err
	}

	return basicBlock, nil
}

func (c *Candidate) getBlocks(cids []string, dss []*types.DownloadSource) ([]blocks.Block, error) {
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

			for i := 0; i < c.retryCount; i++ {
				b, err := c.getBlock(ds, cidStr)
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
