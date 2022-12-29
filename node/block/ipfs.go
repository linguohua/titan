package block

import (
	"context"
	"fmt"
	"io/ioutil"
	"sync"
	"time"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid" // v0.1.0
	"github.com/ipfs/interface-go-ipfs-core/path"
	"github.com/linguohua/titan/node/helper"
)

type IPFS struct{}

func (ipfs *IPFS) loadBlocks(block *Block, reqs []*delayReq) ([]blocks.Block, error) {
	return getBlocksWithDelayReqs(reqs, block)
}

func (ipfs *IPFS) syncData(block *Block, reqs map[int]string) error {
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

	// group cids
	sizeOfGroup := helper.Batch
	groups := make([][]string, 0)
	for i := 0; i < len(cids); i += sizeOfGroup {
		j := i + sizeOfGroup
		if j > len(cids) {
			j = len(cids)
		}

		group := cids[i:j]
		groups = append(groups, group)
	}

	for _, group := range groups {
		blocks, err := getBlocksFromIPFS(block, group)
		if err != nil {
			log.Errorf("syncData getBlocksWithHttp err %v", err)
			return err
		}

		if len(blocks) == 0 {
			return fmt.Errorf("syncData get blocks is empty")
		}

		for _, b := range blocks {
			cidStr := b.Cid().String()
			err = block.saveBlock(ctx, b.RawData(), cidStr, fmt.Sprintf("%d", blockFIDMap[cidStr]))
			if err != nil {
				log.Errorf("syncData save block error:%s", err.Error())
			}
		}
	}

	return nil
}

func getBlockWithIPFSApi(block *Block, cidStr string) (blocks.Block, error) {
	ctx, cancel := context.WithTimeout(context.Background(), helper.BlockDownloadTimeout*time.Second)
	defer cancel()

	reader, err := block.ipfsApi.Block().Get(ctx, path.New(cidStr))
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

func getBlocksFromIPFS(block *Block, cids []string) ([]blocks.Block, error) {
	startTime := time.Now()
	blks := make([]blocks.Block, 0, len(cids))

	var wg sync.WaitGroup

	for _, cid := range cids {
		cidStr := cid
		wg.Add(1)

		go func() {
			defer wg.Done()
			b, err := getBlockWithIPFSApi(block, cidStr)
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

func getBlocksWithDelayReqs(reqs []*delayReq, block *Block) ([]blocks.Block, error) {
	cids := make([]string, 0, len(reqs))

	for _, req := range reqs {
		cids = append(cids, req.blockInfo.Cid)
	}

	return getBlocksFromIPFS(block, cids)

}
