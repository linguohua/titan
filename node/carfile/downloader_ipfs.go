package carfile

import (
	"context"
	"io/ioutil"
	"net/http"
	"sync"
	"time"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid" // v0.1.0
	ipfsApi "github.com/ipfs/go-ipfs-http-client"
	"github.com/ipfs/interface-go-ipfs-core/path"
	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/node/helper"
)

type ipfs struct {
	ipfsApi *ipfsApi.HttpApi
}

func NewIPFS(ipfsApiURL string) *ipfs {
	httpClient := &http.Client{}
	httpApi, err := ipfsApi.NewURLApiWithClient(ipfsApiURL, httpClient)
	if err != nil {
		log.Panicf("NewBlock,NewURLApiWithClient error:%s, url:%s", err.Error(), ipfsApiURL)
	}

	return &ipfs{ipfsApi: httpApi}
}

func (ipfs *ipfs) downloadBlocks(cids []string, sources []*api.DowloadSource) ([]blocks.Block, error) {
	return ipfs.getBlocksFromIPFS(cids)
}

func (ipfs *ipfs) getBlockWithIPFSApi(cidStr string) (blocks.Block, error) {
	ctx, cancel := context.WithTimeout(context.Background(), helper.BlockDownloadTimeout*time.Second)
	defer cancel()

	reader, err := ipfs.ipfsApi.Block().Get(ctx, path.New(cidStr))
	if err != nil {
		return nil, err
	}

	data, err := ioutil.ReadAll(reader)
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

func (ipfs *ipfs) getBlocksFromIPFS(cids []string) ([]blocks.Block, error) {
	startTime := time.Now()
	blks := make([]blocks.Block, 0, len(cids))

	var wg sync.WaitGroup

	for _, cid := range cids {
		cidStr := cid
		wg.Add(1)

		go func() {
			defer wg.Done()
			b, err := ipfs.getBlockWithIPFSApi(cidStr)
			if err != nil {
				log.Errorf("getBlockWithWaitGroup error:%s", err.Error())
				return
			}
			blks = append(blks, b)
		}()
	}
	wg.Wait()

	log.Infof("getBlocksWithHttp block len:%d, duration:%dns", len(blks), time.Since(startTime))
	return blks, nil
}
