package block

import (
	"context"
	"fmt"
	"time"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/linguohua/titan/node/helper"
)

type IPFS struct {
}

func (ipfs *IPFS) loadBlocks(block *Block, req []*delayReq) {
	loadBlocksFromIPFS(block, req)
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
		target, err := cid.Decode(reqData.cid)
		if err != nil {
			log.Errorf("loadBlocksAsync failed to decode CID %v", err)
			continue
		}

		// // convert cid to v0
		// if target.Version() != 0 && target.Type() == cid.DagProtobuf {
		// 	target = cid.NewCidV0(target.Hash())
		// }

		cids = append(cids, target)
		reqMap[reqData.cid] = reqData
	}

	if len(cids) == 0 {
		log.Debug("loadBlocksAsync, len(cids) == 0")
		return
	}

	blocks, err := loadBlocksAsync(block, cids)
	if err != nil {
		log.Errorf("loadBlocksAsync loadBlocks err %v", err)
		return
	}

	var from = ""
	for _, b := range blocks {
		cidStr := b.Cid().String()
		err = block.blockStore.Put(cidStr, b.RawData())
		block.cacheResult(ctx, cidStr, from, err)

		log.Infof("cache data,cid:%s,err:%v", cidStr, err)

		delete(reqMap, cidStr)

		// continue to download block with links
		links, err := block.resolveLinks(b)
		if err != nil {
			log.Errorf("loadBlocksFromIPFS resolveLinks error:%s", err.Error())
			continue
		}

		if len(links) > 0 {
			delayReqs := make([]*delayReq, 0, len(links))
			for _, link := range links {
				dReq := &delayReq{}
				dReq.cid = link.Cid.String()
				delayReqs = append(delayReqs, dReq)
			}

			block.addReq2WaitList(delayReqs)
		}
	}

	if len(reqMap) > 0 {
		err = fmt.Errorf("Request timeout")
		for _, v := range reqMap {
			if v.count > helper.MaxReqCount {
				block.cacheResult(ctx, v.cid, from, err)
				log.Infof("cache data faile, cid:%s, count:%d", v.cid, v.count)
			} else {
				v.count++
				block.addReq2WaitList([]*delayReq{v})
			}
		}
	}
}
