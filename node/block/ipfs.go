package block

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"sync"
	"time"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid" // v0.1.0
	"github.com/ipfs/interface-go-ipfs-core/path"
	"github.com/linguohua/titan/node/helper"
)

type IPFS struct{}

func (ipfs *IPFS) loadBlocks(block *Block, req []*delayReq) {
	loadBlocksFromIPFS(block, req)
}

func (ipfs *IPFS) syncData(block *Block, reqs map[int]string) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if len(reqs) == 0 {
		return nil
	}

	blockFIDMap := make(map[string]int)
	cids := make([]cid.Cid, 0, len(reqs))
	for fid, cidStr := range reqs {
		target, err := cid.Decode(cidStr)
		if err != nil {
			log.Errorf("syncData failed to decode CID %s, error:%s", cidStr, err.Error())
			continue
		}
		cids = append(cids, target)
		blockFIDMap[cidStr] = fid
	}

	// group cids
	sizeOfGroup := helper.Batch
	groups := make([][]cid.Cid, 0)
	for i := 0; i < len(cids); i += sizeOfGroup {
		j := i + sizeOfGroup
		if j > len(cids) {
			j = len(cids)
		}

		group := cids[i:j]
		groups = append(groups, group)
	}

	for _, group := range groups {
		blocks, err := getBlocksWithHttp(block, group)
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

// curl "http://127.0.0.1:8080/ipfs/bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi?format=raw" > cat.jpg
func getBlockFromIPFSGateway(block *Block, cidStr string) (blocks.Block, error) {
	url := fmt.Sprintf("%s/%s?format=raw", block.ipfsGateway, cidStr)
	client := &http.Client{Timeout: helper.BlockDownloadTimeout * time.Second}

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("resp.StatusCode != 200 ")
	}

	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	target, err := cid.Decode(cidStr)
	if err != nil {
		return nil, err
	}

	basicBlock, err := blocks.NewBlockWithCid(data, target)
	if err != nil {
		log.Fatalf("NewBlockWithCid err:%s", err.Error())
	}

	return basicBlock, nil
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

func getBlocksWithHttp(block *Block, cids []cid.Cid) ([]blocks.Block, error) {
	startTime := time.Now()
	blks := make([]blocks.Block, 0, len(cids))

	var wg sync.WaitGroup

	for _, cid := range cids {
		cidStr := cid.String()
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

func loadBlocksAsync(block *Block, cids []cid.Cid) ([]blocks.Block, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	blockCh, err := block.exchange.GetBlocks(ctx, cids)
	if err != nil {
		log.Errorf("CacheData, loadBlock error:", err)
		return nil, err
	}

	results := make([]blocks.Block, 0, len(cids))
	for block := range blockCh {
		results = append(results, block)
	}
	// log.Debug("get block end")
	return results, nil
}

func loadBlocksFromIPFS(block *Block, req []*delayReq) {
	req = block.filterAvailableReq(req)
	ctx := context.Background()

	cids := make([]cid.Cid, 0, len(req))
	reqMap := make(map[string]*delayReq)
	for _, reqData := range req {
		target, err := cid.Decode(reqData.blockInfo.Cid)
		if err != nil {
			log.Errorf("loadBlocksAsync failed to decode CID %v", err)
			continue
		}

		// // convert cid to v0
		// if target.Version() != 0 && target.Type() == cid.DagProtobuf {
		// 	target = cid.NewCidV0(target.Hash())
		// }

		cids = append(cids, target)
		reqMap[reqData.blockInfo.Cid] = reqData
	}

	if len(cids) == 0 {
		log.Debug("loadBlocksAsync, len(cids) == 0")
		return
	}

	blocks, err := getBlocksWithHttp(block, cids)
	if err != nil {
		log.Errorf("loadBlocksAsync loadBlocks err %v", err)
		return
	}

	for _, b := range blocks {
		cidStr := b.Cid().String()
		req, ok := reqMap[cidStr]
		if !ok {
			log.Errorf("loadBlocksFromIPFS cid %s not in map", cidStr)
			continue
		}

		err = block.saveBlock(ctx, b.RawData(), req.blockInfo.Cid, fmt.Sprintf("%d", req.blockInfo.Fid))
		if err != nil {
			log.Errorf("loadBlocksFromIPFS save block error:%s", err.Error())
			continue
		}

		// get block links
		links, err := block.resolveLinks(b)
		if err != nil {
			log.Errorf("loadBlocksFromIPFS resolveLinks error:%s", err.Error())
			continue
		}

		linksSize := uint64(0)
		cids := make([]string, 0, len(links))
		for _, link := range links {
			cids = append(cids, link.Cid.String())
			linksSize += link.Size
		}

		bStat := blockStat{cid: cidStr, links: cids, blockSize: len(b.RawData()), linksSize: linksSize, carFileHash: req.carFileHash, CacheID: req.CacheID}
		block.cacheResult(bStat, nil)

		log.Infof("cache data,cid:%s,err:%v", cidStr, err)

		delete(reqMap, cidStr)
	}

	err = fmt.Errorf("Request timeout")
	tryDelayReqs := make([]*delayReq, 0)
	for _, v := range reqMap {
		if v.count >= helper.BlockDownloadRetryNum {
			block.cacheResultWithError(blockStat{cid: v.blockInfo.Cid, carFileHash: v.carFileHash, CacheID: v.CacheID}, err)
			log.Infof("cache data faile, cid:%s, count:%d", v.blockInfo.Cid, v.count)
		} else {
			v.count++
			delayReq := v
			tryDelayReqs = append(tryDelayReqs, delayReq)
		}
	}

	if len(tryDelayReqs) > 0 {
		loadBlocksFromIPFS(block, tryDelayReqs)
	}
}
