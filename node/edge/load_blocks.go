package edge

import (
	"context"
	"fmt"
	"time"

	"github.com/linguohua/titan/api"
)

type delayReq struct {
	cid   string
	count int
	// use for edge node load block
	candidateURL string
}

func startBlockLoader(ctx context.Context, edge EdgeAPI) {
	for {
		doLen := len(reqList)
		if doLen == 0 {
			cachingList = nil
			time.Sleep(10 * time.Millisecond)
			continue
		}

		if doLen > batch {
			doLen = batch
		}

		doReqs := reqList[:doLen]
		reqList = reqList[doLen:]
		cachingList = doReqs

		loadBlocks(edge, doReqs)
	}
}

func loadBlocks(edge EdgeAPI, req []delayReq) {
	if edge.isCandidate {
		loadBlocksFromIPFS(edge, req)
	} else {
		loadBlocksFromCandidate(edge, req)
	}
}

func apiReq2DelayReq(req api.ReqCacheData) []delayReq {
	results := make([]delayReq, 0, len(req.Cids))
	for _, cid := range req.Cids {
		req := delayReq{cid: cid, count: 0, candidateURL: req.CandidateURL}
		results = append(results, req)
	}

	return results
}

func cacheResult(ctx context.Context, edge EdgeAPI, cid, from string, err error) {
	var errMsg = ""
	var success = true
	if err != nil {
		success = true
		errMsg = err.Error()
	}

	result := api.CacheResultInfo{Cid: cid, IsOK: success, Msg: errMsg, From: from}
	fid, err := edge.scheduler.CacheResult(ctx, edge.DeviceAPI.DeviceID, result)
	if err != nil {
		log.Errorf("load_block CacheResult error:%v", err)
		return
	}

	if success && fid != "" {
		oldFid, _ := getFID(edge, cid)
		if oldFid != "" {
			// delete old fid key
			err = edge.ds.Delete(ctx, newKeyFID(oldFid))
			if err != nil {
				log.Errorf("DeleteData, delete key fid %s error:%v", fid, err)
			}
		}

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

	var from = ""
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
			cacheResult(ctx, edge, reqData.cid, from, nil)
			continue
		}
		results = append(results, reqData)
	}

	return results
}
