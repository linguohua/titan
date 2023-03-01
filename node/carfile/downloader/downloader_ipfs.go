package downloader

import (
	"context"
	"io/ioutil"
	"net/http"
	"sync"
	"time"

	blocks "github.com/ipfs/go-block-format" // v0.1.0
	"github.com/ipfs/go-cid"
	httpapi "github.com/ipfs/go-ipfs-http-client"
	"github.com/ipfs/interface-go-ipfs-core/path"
	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/node/carfile/carfilestore"
	"github.com/linguohua/titan/node/cidutil"
)

type ipfs struct {
	httpAPI      *httpapi.HttpApi
	carfileStore *carfilestore.CarfileStore
}

func NewIPFS(ipfsApiURL string, cs *carfilestore.CarfileStore) *ipfs {
	httpAPI, err := httpapi.NewURLApiWithClient(ipfsApiURL, &http.Client{})
	if err != nil {
		log.Panicf("NewBlock,NewURLApiWithClient error:%s, url:%s", err.Error(), ipfsApiURL)
	}

	return &ipfs{httpAPI: httpAPI, carfileStore: cs}
}

func (ipfs *ipfs) DownloadBlocks(cids []string, dss []*api.DownloadSource) ([]blocks.Block, error) {
	return ipfs.getBlocks(cids)
}

func (ipfs *ipfs) getBlock(cidStr string) (blocks.Block, error) {
	blockHash, err := cidutil.CIDString2HashString(cidStr)
	if err != nil {
		return nil, err
	}

	data, err := ipfs.carfileStore.Block(blockHash)
	if err == nil { // continue download block if err not nil
		return newBlock(cidStr, data)
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout*time.Second)
	defer cancel()

	reader, err := ipfs.httpAPI.Block().Get(ctx, path.New(cidStr))
	if err != nil {
		return nil, err
	}

	data, err = ioutil.ReadAll(reader)
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

			for i := 0; i < retryCount; i++ {
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
