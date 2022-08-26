package edge

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/api/client"
	"github.com/linguohua/titan/build"
	"github.com/linguohua/titan/lib/p2p"
	"github.com/linguohua/titan/stores"
	"golang.org/x/time/rate"

	"github.com/ipfs/go-datastore"
	exchange "github.com/ipfs/go-ipfs-exchange-interface"
	"github.com/linguohua/titan/node/common"
	"github.com/linguohua/titan/node/device"
)

func NewLocalEdgeNode(ctx context.Context, ds datastore.Batching, scheduler api.Scheduler, blockStore stores.BlockStore, device device.DeviceAPI, isCandidate bool) api.Edge {
	addrs, err := build.BuiltinBootstrap()
	if err != nil {
		log.Fatal(err)
	}

	exchange, err := p2p.Bootstrap(ctx, addrs)
	if err != nil {
		log.Fatal(err)
	}
	edge := EdgeAPI{
		ds:          ds,
		scheduler:   scheduler,
		blockStore:  blockStore,
		limiter:     rate.NewLimiter(rate.Inf, 0),
		exchange:    exchange,
		DeviceAPI:   device,
		isCandidate: isCandidate,
	}

	go startBlockLoader(ctx, edge)
	return edge
}

type EdgeAPI struct {
	common.CommonAPI
	device.DeviceAPI
	ds          datastore.Batching
	blockStore  stores.BlockStore
	scheduler   api.Scheduler
	limiter     *rate.Limiter
	exchange    exchange.Interface
	isCandidate bool
}

func (edge EdgeAPI) GetSchedulerAPI() api.Scheduler {
	log.Info("GetSchedulerAPI")
	return edge.scheduler
}

func (edge EdgeAPI) WaitQuiet(ctx context.Context) error {
	log.Info("WaitQuiet")
	return nil
}

func (edge EdgeAPI) CacheData(ctx context.Context, req api.ReqCacheData) error {
	log.Infof("CacheData, req len:%d", len(req.Cids))
	if edge.blockStore == nil {
		return fmt.Errorf("CacheData, blockStore == nil ")
	}

	// cache data for candidate
	delayReq := filterAvailableReq(edge, apiReq2DelayReq(req))
	if len(delayReq) == 0 {
		log.Infof("CacheData, len(req) == 0 not need to handle")
		return nil
	}

	reqList = append(reqList, delayReq...)

	// go loadBlocksOneByOne(edge, req)
	return nil
}

func (edge EdgeAPI) BlockStoreStat(ctx context.Context) error {
	log.Info("BlockStoreStat")

	return nil
}

func (edge EdgeAPI) LoadData(ctx context.Context, cid string) ([]byte, error) {
	log.Info("LoadData")

	if edge.blockStore == nil {
		log.Errorf("LoadData, blockStore not setting")
		return nil, nil
	}

	return edge.blockStore.Get(cid)
}

func (edge EdgeAPI) DoVerify(ctx context.Context, reqVerify api.ReqVerify, candidateURL string) error {
	log.Info("DoVerify")

	if edge.blockStore == nil {
		log.Errorf("DoVerify,edge.blockStore == nil ")
		return fmt.Errorf("edge.blockStore == nil")
	}

	fids := reqVerify.FIDs

	if len(fids) == 0 {
		log.Errorf("len(fids) == 0")
		return fmt.Errorf("len(fids) == 0")
	}

	candidateAPI, closer, err := client.NewCandicate(ctx, candidateURL, nil)
	if err != nil {
		log.Errorf("DoVerify, NewCandicate err:%v", err)
		return err
	}
	defer closer()

	r := rand.New(rand.NewSource(reqVerify.Seed))
	t := time.NewTimer(time.Duration(reqVerify.Duration) * time.Second)

	for {
		select {
		case <-t.C:
			return nil
		default:
		}

		random := r.Intn(len(fids))
		fidStr := fids[random]
		err = sendBlock(ctx, edge, candidateAPI, fidStr)
		if err != nil {
			return err
		}
	}
}

func sendBlock(ctx context.Context, edge EdgeAPI, candidateAPI api.Candidate, fid string) error {
	var block []byte
	cid, err := getCID(edge, fid)
	if err != nil {
		if err == datastore.ErrNotFound {
			log.Infof("sendBlock, fid %s not found", fid)
			block = nil
		} else {
			return err
		}
	}

	if cid != "" {
		block, err = edge.blockStore.Get(cid)
		if err != nil {
			if err == datastore.ErrNotFound {
				log.Infof("sendBlock, cid %s not found, fid:%s", cid, fid)
				block = nil
			} else {
				log.Errorf("sendBlock, get block err:%v", err)
				return err
			}
		}
	}

	return candidateAPI.SendBlock(ctx, block, edge.DeviceID)
}

// call by scheduler
func (edge EdgeAPI) DeleteData(ctx context.Context, cids []string) (api.DelResult, error) {
	log.Info("DeleteData")
	delResult := api.DelResult{}
	delResult.List = make([]api.DelFailed, 0)

	if edge.blockStore == nil {
		log.Errorf("DeleteData, blockStore not setting")
		return delResult, fmt.Errorf("edge.blockStore == nil")
	}

	for _, cid := range cids {
		err := edge.blockStore.Delete(cid)
		if err == datastore.ErrNotFound {
			continue
		}

		if err != nil {
			result := api.DelFailed{Cid: cid, ErrMsg: err.Error()}
			delResult.List = append(delResult.List, result)
			log.Errorf("DeleteData, delete block %s error:%v", cid, err)
			continue
		}

		fid, err := getFID(edge, cid)
		if err != nil {
			log.Errorf("DeleteData, get fid from cid %s error:%v", cid, err)
			continue
		}

		err = edge.ds.Delete(ctx, newKeyFID(fid))
		if err != nil {
			log.Errorf("DeleteData, delete key fid %s error:%v", fid, err)
		}
		err = edge.ds.Delete(ctx, newKeyCID(cid))
		if err != nil {
			log.Errorf("DeleteData, delete key cid %s error:%v", cid, err)
		}
	}
	return delResult, nil
}

// call by edge or candidate
func (edge EdgeAPI) DeleteBlocks(ctx context.Context, cids []string) (api.DelResult, error) {
	log.Info("DeleteBlock")
	delResult := api.DelResult{}
	delResult.List = make([]api.DelFailed, 0)

	result, err := edge.scheduler.DeleteDataRecord(ctx, edge.DeviceID, cids)
	if err != nil {
		log.Errorf("DeleteBlock, delete block error:%v", err)
		return delResult, err
	}

	for _, cid := range cids {
		_, ok := result[cid]
		if ok {
			continue
		}

		err = edge.blockStore.Delete(cid)
		if err != nil {
			result[cid] = err.Error()
		}

		fid, err := getFID(edge, cid)
		if err != nil {
			log.Errorf("DeleteData, get fid from cid %s error:%v", cid, err)
			continue
		}

		err = edge.ds.Delete(ctx, newKeyFID(fid))
		if err != nil {
			log.Errorf("DeleteData, delete key fid %s error:%v", fid, err)
		}
		err = edge.ds.Delete(ctx, newKeyCID(cid))
		if err != nil {
			log.Errorf("DeleteData, delete key cid %s error:%v", cid, err)
		}
	}

	for k, v := range result {
		log.Errorf("DeleteBlock, delete block %s error:%v", k, v)
		result := api.DelFailed{Cid: k, ErrMsg: v}
		delResult.List = append(delResult.List, result)
	}

	return delResult, nil
}

func (edge EdgeAPI) QueryCacheStat(ctx context.Context) (api.CacheStat, error) {
	result := api.CacheStat{}

	keyCount, err := edge.blockStore.KeyCount()
	if err != nil {
		log.Errorf("block store key count error:%v", err)
	}

	result.CacheBlockCount = keyCount
	result.WaitCacheBlockNum = len(reqList)
	result.DoingCacheBlockNum = len(cachingList)

	log.Infof("CacheBlockCount:%d,WaitCacheBlockNum:%d, DoingCacheBlockNum:%d", result.CacheBlockCount, result.WaitCacheBlockNum, result.DoingCacheBlockNum)
	return result, nil
}

func (edge EdgeAPI) QueryCachingBlocks(ctx context.Context) (api.CachingBlockList, error) {
	result := api.CachingBlockList{}
	return result, nil
}

func (edge EdgeAPI) SetDownloadSpeed(ctx context.Context, speedRate int64) error {
	log.Infof("set download speed %d", speedRate)
	if edge.limiter == nil {
		return fmt.Errorf("edge.limiter == nil")
	}
	edge.limiter.SetLimit(rate.Limit(speedRate))
	edge.limiter.SetBurst(int(speedRate))

	return nil
}

func (edge EdgeAPI) UnlimitDownloadSpeed(ctx context.Context) error {
	log.Infof("UnlimitDownloadSpeed")
	if edge.limiter == nil {
		return fmt.Errorf("edge.limiter == nil")
	}

	edge.limiter.SetLimit(rate.Inf)
	edge.limiter.SetBurst(0)

	return nil
}
