package edge

import (
	"context"
	"fmt"
	"time"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/linguohua/titan/api"
)

type delayReq struct {
	cid   string
	count int
}

func apiReq2DelayReq(req api.ReqCacheData) []delayReq {
	results := make([]delayReq, 0, len(req.Cids))
	for _, cid := range req.Cids {
		req := delayReq{cid: cid, count: 0}
		results = append(results, req)
	}

	return results
}

func startLoadBlockFromIPFS(ctx context.Context, edge EdgeAPI) {
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
	fid, err := edge.scheduler.CacheResult(ctx, edge.DeviceAPI.DeviceID, api.CacheResultInfo{Cid: cid, IsOK: success})
	if err != nil {
		log.Errorf("load_block CacheResult error:%v", err)
		return
	}

	if success && fid != "" {
		err = edge.ds.Put(ctx, newKeyFID(fid), []byte(cid))
		if err != nil {
			log.Errorf("load_block CacheResult save fid error:%v", err)
		}

		err = edge.ds.Put(ctx, newKeyCID(cid), []byte(fid))
		if err != nil {
			log.Errorf("load_block CacheResult save cid error:%v", err)
		}

	}

	// log.Infof("cacheResult fid:%s", fid)
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

		cidStr := fmt.Sprintf("%s", reqData.cid)

		has, _ := edge.blockStore.Has(cidStr)
		if has {
			cacheResult(ctx, edge, reqData.cid, true)
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

	blocks, err := loadBlocks(edge, cids)
	if err != nil {
		log.Errorf("loadBlocksAsync loadBlocks err %v", err)
		return
	}

	for _, block := range blocks {
		cidStr := fmt.Sprintf("%v", block.Cid())
		err = edge.blockStore.Put(cidStr, block.RawData())
		cacheResult(ctx, edge, cidStr, err == nil)

		log.Infof("cache data,cid:%s,err:%v", cidStr, err)

		delete(reqMap, cidStr)
	}

	if len(reqMap) > 0 {
		for _, v := range reqMap {
			if v.count > 5 {
				cacheResult(ctx, edge, v.cid, false)
				log.Infof("cache data faile, cid:%s, count:%d", v.cid, v.count)
			} else {
				v.count++
				reqDatas = append(reqDatas, v)
			}
		}
	}
}
