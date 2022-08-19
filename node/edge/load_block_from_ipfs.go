package edge

import (
	"context"
	"fmt"
	"time"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/linguohua/titan/api"
)

var (
	batch       = 10
	reqDatas    []delayReq
	maxReqCount = 5
)

type delayReq struct {
	req   api.ReqCacheData
	count int
}

func apiReq2DelayReq(reqs []api.ReqCacheData) []delayReq {
	results := make([]delayReq, 0, len(reqs))
	for _, reqData := range reqs {
		req := delayReq{req: reqData, count: 0}
		results = append(results, req)
	}

	return results
}

func startBlockLoader(ctx context.Context, edge EdgeAPI) {
	for {
		doLen := len(reqDatas)
		if doLen == 0 {
			time.Sleep(10 * time.Millisecond)
			continue
		}

		if doLen > batch {
			doLen = batch
		}

		doReqs := reqDatas[:doLen]
		reqDatas = reqDatas[doLen:]

		loadBlocksAsync(edge, doReqs)
	}
}

func loadBlocks(edge EdgeAPI, cids []cid.Cid) ([]blocks.Block, error) {
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

func cacheResult(ctx context.Context, edge EdgeAPI, cid string, success bool) {
	_, err := edge.scheduler.CacheResult(ctx, edge.DeviceAPI.DeviceID, cid, success)
	if err != nil {
		log.Errorf("load_block CacheResult error:%v", err)
	}
}

func filterAvailableReq(edge EdgeAPI, reqs []delayReq) []delayReq {
	ctx := context.Background()

	results := make([]delayReq, 0, len(reqs))
	for _, reqData := range reqs {
		// target, err := cid.Decode(reqData.Cid)
		// if err != nil {
		// 	log.Errorf("loadBlocksAsync failed to decode CID %v", err)
		// 	continue
		// }

		// // convert cid to v0
		// if target.Version() != 0 && target.Type() == cid.DagProtobuf {
		// 	target = cid.NewCidV0(target.Hash())
		// }

		cidStr := fmt.Sprintf("%s", reqData.req.Cid)

		has, _ := edge.blockStore.Has(cidStr)
		if has {
			exist, err := edge.ds.Has(ctx, datastore.NewKey(reqData.req.ID))
			if err == nil && !exist {
				edge.ds.Put(ctx, datastore.NewKey(reqData.req.ID), []byte(cidStr))
			}

			cacheResult(ctx, edge, reqData.req.Cid, true)
			continue
		}
		results = append(results, reqData)
	}

	return results
}

func loadBlocksAsync(edge EdgeAPI, req []delayReq) {
	req = filterAvailableReq(edge, req)
	ctx := context.Background()

	cids := make([]cid.Cid, 0, len(req))
	reqMap := make(map[string]delayReq)
	for _, reqData := range req {
		target, err := cid.Decode(reqData.req.Cid)
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

	blocks, err := loadBlocks(edge, cids)
	if err != nil {
		log.Errorf("loadBlocksAsync loadBlocks err %v", err)
		return
	}

	for _, block := range blocks {
		cidStr := fmt.Sprintf("%v", block.Cid())
		err = edge.blockStore.Put(cidStr, block.RawData())
		if err == nil {
			err = edge.ds.Put(ctx, datastore.NewKey(reqMap[cidStr].req.ID), []byte(cidStr))
		}

		cacheResult(ctx, edge, cidStr, err == nil)

		log.Infof("cache data,cid:%s,err:%v", cidStr, err)

		delete(reqMap, cidStr)
	}

	if len(reqMap) > 0 {
		for _, v := range reqMap {
			if v.count > 5 {
				cacheResult(ctx, edge, v.req.Cid, err == nil)
				log.Infof("cache data faile, cid:%s, count:%d", v.req.Cid, v.count)
			} else {
				v.count++
				reqDatas = append(reqDatas, v)
			}
		}
	}
}
