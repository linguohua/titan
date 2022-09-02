package edge

import (
	"context"
	"fmt"
	"time"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
)

func loadBlocksAsync(edge *Edge, cids []cid.Cid) ([]blocks.Block, error) {
	for _, id := range cids {
		log.Infof("loadBlocks id:%v", id)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	blockCh, err := edge.exchange.GetBlocks(ctx, cids)
	if err != nil {
		log.Errorf("CacheData, loadBlock error:", err)
		return nil, err
	}

	results := make([]blocks.Block, 0, len(cids))
	for block := range blockCh {
		results = append(results, block)
	}
	log.Infof("get block end")
	return results, nil
}

func loadBlocksFromIPFS(edge *Edge, req []*delayReq) {
	req = filterAvailableReq(edge, req)
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

		cidStr := fmt.Sprintf("%v", target)
		reqMap[cidStr] = reqData
	}

	if len(cids) == 0 {
		log.Infof("loadBlocksAsync, len(cids) == 0")
		return
	}

	blocks, err := loadBlocksAsync(edge, cids)
	if err != nil {
		log.Errorf("loadBlocksAsync loadBlocks err %v", err)
		return
	}

	var from = ""
	for _, block := range blocks {
		cidStr := fmt.Sprintf("%v", block.Cid())
		err = edge.blockStore.Put(cidStr, block.RawData())
		cacheResult(ctx, edge, cidStr, from, err)

		log.Infof("cache data,cid:%s,err:%v", cidStr, err)

		delete(reqMap, cidStr)
	}

	if len(reqMap) > 0 {
		err = fmt.Errorf("Request timeout")
		for _, v := range reqMap {
			if v.count > maxReqCount {
				cacheResult(ctx, edge, v.cid, from, err)
				log.Infof("cache data faile, cid:%s, count:%d", v.cid, v.count)
			} else {
				v.count++
				edge.reqList = append(edge.reqList, v)
			}
		}
	}
}
