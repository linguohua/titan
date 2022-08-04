package edge

import (
	"context"
	"os"
	"time"

	CID "github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/linguohua/titan/api"
)

func getBlock(edge EdgeAPI, cid string) ([]byte, error) {
	target, err := CID.Decode(cid)
	if err != nil {
		log.Errorf("failed to decode CID '%q': %s", os.Args[2], err)
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	block, err := edge.exchange.GetBlock(ctx, target)
	if err != nil {
		log.Errorf("CacheData, loadBlock error:", err)
		return nil, err
	}

	return block.RawData(), nil
}

func cacheResult(ctx context.Context, edge EdgeAPI, cid string, success bool) {
	err := edge.scheduler.CacheResult(ctx, edge.deviceID, cid, success)
	if err != nil {
		log.Errorf("load_block CacheResult error:", err)
	}
}

func loadBlocks(edge EdgeAPI, req []api.ReqCacheData) {
	ctx := context.Background()

	for _, reqData := range req {
		has, err := edge.blockStore.Has(reqData.Cid)
		if err == nil && has {
			cacheResult(ctx, edge, reqData.Cid, true)
			continue
		}

		block, err := getBlock(edge, reqData.Cid)
		if err != nil {
			log.Errorf("CacheData, loadBlock error:", err)
			cacheResult(ctx, edge, reqData.Cid, false)
			continue
		}

		err = edge.blockStore.Put(reqData.Cid, block)
		if err == nil {
			err = edge.ds.Put(ctx, datastore.NewKey(reqData.ID), []byte(reqData.Cid))
		}

		cacheResult(ctx, edge, reqData.Cid, err == nil)

		log.Infof("cache data,cid:%s,err:%v", reqData.Cid, err)
	}
}
