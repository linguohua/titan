package downloader

import (
	"context"
	"io/ioutil"
	"net/http"
	"sync"
	"time"

	blocks "github.com/ipfs/go-block-format" // v0.1.0
	ipfsApi "github.com/ipfs/go-ipfs-http-client"
	"github.com/ipfs/interface-go-ipfs-core/path"
	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/node/carfile/carfilestore"
	"github.com/linguohua/titan/node/cidutil"
)

type ipfs struct {
	ipfsApi      *ipfsApi.HttpApi
	carfileStore *carfilestore.CarfileStore
}

func NewIPFS(ipfsApiURL string, carfileStore *carfilestore.CarfileStore) *ipfs {
	httpClient := &http.Client{}
	httpApi, err := ipfsApi.NewURLApiWithClient(ipfsApiURL, httpClient)
	if err != nil {
		log.Panicf("NewBlock,NewURLApiWithClient error:%s, url:%s", err.Error(), ipfsApiURL)
	}

	return &ipfs{ipfsApi: httpApi, carfileStore: carfileStore}
}

func (ipfs *ipfs) DownloadBlocks(cids []string, sources []*api.DowloadSource) ([]blocks.Block, error) {
	return ipfs.getBlocksFromIPFS(cids)
}

func (ipfs *ipfs) getBlockWithIPFSApi(cidStr string, retryCount int) (blocks.Block, error) {
	blockHash, err := cidutil.CIDString2HashString(cidStr)
	if err != nil {
		return nil, err
	}

	data, err := ipfs.carfileStore.GetBlock(blockHash)
	if err == nil {
		return newBlock(cidStr, data)
	}

	ctx, cancel := context.WithTimeout(context.Background(), blockDownloadTimeout*time.Second)
	defer cancel()

	reader, err := ipfs.ipfsApi.Block().Get(ctx, path.New(cidStr))
	if err != nil {
		if retryCount < blockDownloadRetryNum {
			retryCount++
			return ipfs.getBlockWithIPFSApi(cidStr, retryCount)
		}
		return nil, err
	}

	data, err = ioutil.ReadAll(reader)
	if err != nil {
		return nil, err
	}

	return newBlock(cidStr, data)
}

func (ipfs *ipfs) getBlocksFromIPFS(cids []string) ([]blocks.Block, error) {
	blks := make([]blocks.Block, 0, len(cids))
	blksLock := &sync.Mutex{}

	var wg sync.WaitGroup

	for _, cid := range cids {
		cidStr := cid
		wg.Add(1)

		go func() {
			defer wg.Done()
			b, err := ipfs.getBlockWithIPFSApi(cidStr, 0)
			if err != nil {
				log.Errorf("getBlockWithWaitGroup error:%s", err.Error())
				return
			}

			blksLock.Lock()
			blks = append(blks, b)
			blksLock.Unlock()
		}()
	}
	wg.Wait()

	return blks, nil
}
