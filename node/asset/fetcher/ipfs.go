package fetcher

import (
	"context"
	"io/ioutil"
	"net/http"
	"sync"
	"time"

	"github.com/ipfs/go-cid"
	httpapi "github.com/ipfs/go-ipfs-http-client"
	"github.com/ipfs/go-libipfs/blocks"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipfs/interface-go-ipfs-core/path"
	"github.com/linguohua/titan/api/types"
)

var log = logging.Logger("asset/fetcher")

type IPFS struct {
	httpAPI    *httpapi.HttpApi
	timeout    int
	retryCount int
}

func NewIPFS(ipfsAPIURL string, timeout, retryCount int) *IPFS {
	httpAPI, err := httpapi.NewURLApiWithClient(ipfsAPIURL, &http.Client{})
	if err != nil {
		log.Panicf("new ipfs error:%s, url:%s", err.Error(), ipfsAPIURL)
	}

	return &IPFS{httpAPI: httpAPI, timeout: timeout, retryCount: retryCount}
}

func (ipfs *IPFS) Fetch(ctx context.Context, cids []string, dss []*types.CandidateDownloadInfo) ([]blocks.Block, error) {
	return ipfs.getBlocks(ctx, cids)
}

func (ipfs *IPFS) getBlock(cidStr string) (blocks.Block, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(ipfs.timeout)*time.Second)
	defer cancel()

	reader, err := ipfs.httpAPI.Block().Get(ctx, path.New(cidStr))
	if err != nil {
		return nil, err
	}

	data, err := ioutil.ReadAll(reader)
	if err != nil {
		return nil, err
	}

	return newBlock(cidStr, data)
}

func (ipfs *IPFS) getBlocks(ctx context.Context, cids []string) ([]blocks.Block, error) {
	blks := make([]blocks.Block, 0, len(cids))
	blksLock := &sync.Mutex{}

	var wg sync.WaitGroup

	for _, cid := range cids {
		cidStr := cid
		wg.Add(1)

		go func() {
			defer wg.Done()

			for i := 0; i < ipfs.retryCount; i++ {
				b, err := ipfs.getBlock(cidStr)
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

func newBlock(cidStr string, data []byte) (blocks.Block, error) {
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
