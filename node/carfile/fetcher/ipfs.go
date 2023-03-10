package fetcher

import (
	"context"
	"io/ioutil"
	"net/http"
	"sync"
	"time"

	blocks "github.com/ipfs/go-block-format" // v0.1.0
	"github.com/ipfs/go-cid"
	httpapi "github.com/ipfs/go-ipfs-http-client"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipfs/interface-go-ipfs-core/path"
	"github.com/linguohua/titan/api/types"
)

var log = logging.Logger("carfile/fetcher")

type ipfs struct {
	httpAPI    *httpapi.HttpApi
	timeout    int
	retryCount int
	// carfileStore *carfilestore.CarfileStore
}

func NewIPFS(ipfsApiURL string, timeout, retryCount int) *ipfs {
	httpAPI, err := httpapi.NewURLApiWithClient(ipfsApiURL, &http.Client{})
	if err != nil {
		log.Panicf("NewBlock,NewURLApiWithClient error:%s, url:%s", err.Error(), ipfsApiURL)
	}

	return &ipfs{httpAPI: httpAPI, timeout: timeout, retryCount: retryCount}
}

func (ipfs *ipfs) Fetch(ctx context.Context, cids []string, dss []*types.DownloadSource) ([]blocks.Block, error) {
	return ipfs.getBlocks(cids)
}

func (ipfs *ipfs) getBlock(cidStr string) (blocks.Block, error) {
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

func (ipfs *ipfs) getBlocks(cids []string) ([]blocks.Block, error) {
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
