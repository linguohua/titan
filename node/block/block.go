package block

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/ipfs/go-datastore"
	exchange "github.com/ipfs/go-ipfs-exchange-interface"
	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/node/helper"
	"github.com/linguohua/titan/stores"

	logging "github.com/ipfs/go-log/v2"
)

var log = logging.Logger("block")

type delayReq struct {
	cid   string
	count int
	// use for edge node load block
	candidateURL string
}

type Block struct {
	ds              datastore.Batching
	blockStore      stores.BlockStore
	scheduler       api.Scheduler
	reqList         []*delayReq
	cachingList     []*delayReq
	cacheResultLock *sync.Mutex
	block           BlockInterface
	deviceID        string
	exchange        exchange.Interface
}

// TODO need to rename
type BlockInterface interface {
	loadBlocks(block *Block, req []*delayReq)
}

func NewBlock(ds datastore.Batching, blockStore stores.BlockStore, scheduler api.Scheduler, blockInterface BlockInterface, exchange exchange.Interface, deviceID string) *Block {
	block := &Block{
		ds:         ds,
		blockStore: blockStore,
		scheduler:  scheduler,
		block:      blockInterface,
		exchange:   exchange,
		deviceID:   deviceID,

		cacheResultLock: &sync.Mutex{},
	}
	go block.startBlockLoader()

	return block
}

func apiReq2DelayReq(req *api.ReqCacheData) []*delayReq {
	results := make([]*delayReq, 0, len(req.Cids))
	for _, cid := range req.Cids {
		req := &delayReq{cid: cid, count: 0, candidateURL: req.CandidateURL}
		results = append(results, req)
	}

	return results
}

func (block *Block) startBlockLoader() {
	for {
		doLen := len(block.reqList)
		if doLen == 0 {
			block.cachingList = nil
			time.Sleep(time.Duration(helper.LoadBockTick) * time.Millisecond)
			continue
		}

		if doLen > helper.Batch {
			doLen = helper.Batch
		}

		doReqs := block.reqList[:doLen]
		block.reqList = block.reqList[doLen:]
		block.cachingList = doReqs

		block.block.loadBlocks(block, doReqs)
	}
}

func (block *Block) cacheResult(ctx context.Context, cid, from string, err error) {
	block.cacheResultLock.Lock()
	defer block.cacheResultLock.Unlock()

	errMsg := ""
	success := true
	if err != nil {
		success = false
		errMsg = err.Error()
	}

	result := api.CacheResultInfo{Cid: cid, IsOK: success, Msg: errMsg, From: from}
	fid, err := block.scheduler.CacheResult(ctx, block.deviceID, result)
	if err != nil {
		log.Errorf("load_block CacheResult error:%v", err)
	}

	if success && fid != "" {
		oldCid, _ := block.getCID(fid)
		if len(oldCid) != 0 && oldCid != cid {
			log.Infof("delete old cid:%s, new cid:%s", oldCid, cid)
			err = block.ds.Delete(ctx, helper.NewKeyCID(oldCid))
			if err != nil {
				log.Errorf("DeleteData, delete key fid %s error:%v", fid, err)
			}
		}

		oldFid, _ := block.getFID(cid)
		if oldFid != "" {
			// delete old fid key
			log.Infof("delete old fid:%s, new fid:%s", oldFid, fid)
			err = block.ds.Delete(ctx, helper.NewKeyFID(oldFid))
			if err != nil {
				log.Errorf("DeleteData, delete key fid %s error:%v", fid, err)
			}
		}

		err = block.ds.Put(ctx, helper.NewKeyFID(fid), []byte(cid))
		if err != nil {
			log.Errorf("load_block CacheResult save fid error:%v", err)
		}

		err = block.ds.Put(ctx, helper.NewKeyCID(cid), []byte(fid))
		if err != nil {
			log.Errorf("load_block CacheResult save cid error:%v", err)
		}

	}
}

func (block *Block) filterAvailableReq(reqs []*delayReq) []*delayReq {
	ctx := context.Background()

	from := ""
	results := make([]*delayReq, 0, len(reqs))
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

		has, _ := block.blockStore.Has(cidStr)
		if has {
			block.cacheResult(ctx, reqData.cid, from, nil)
			continue
		}
		results = append(results, reqData)
	}

	return results
}

func (block *Block) CacheBlocks(ctx context.Context, req api.ReqCacheData) error {
	delayReq := block.filterAvailableReq(apiReq2DelayReq(&req))
	if len(delayReq) == 0 {
		log.Debug("CacheData, len(req) == 0 not need to handle")
		return nil
	}

	block.reqList = append(block.reqList, delayReq...)

	return nil
}

// delete block in local store and scheduler
func (block *Block) DeleteBlocks(ctx context.Context, cids []string) ([]api.BlockOperationResult, error) {
	log.Debug("DeleteBlocks")
	// delResult := api.DelResult{}
	results := make([]api.BlockOperationResult, 0)

	if block.blockStore == nil {
		log.Errorf("DeleteBlocks, blockStore not setting")
		return results, fmt.Errorf("edge.blockStore == nil")
	}

	for _, cid := range cids {
		err := block.blockStore.Delete(cid)
		if err == datastore.ErrNotFound {
			continue
		}

		if err != nil {
			result := api.BlockOperationResult{Cid: cid, ErrMsg: err.Error()}
			results = append(results, result)
			log.Errorf("DeleteBlocks, delete block %s error:%v", cid, err)
			continue
		}
	}
	return results, nil
}

// told to scheduler, local block was delete
func (block *Block) AnnounceBlocksWasDelete(ctx context.Context, cids []string) ([]api.BlockOperationResult, error) {
	log.Debug("AnnounceBlocksWasDelete")
	// delResult := api.DelResult{}
	failedResults := make([]api.BlockOperationResult, 0)

	result, err := block.scheduler.DeleteBlockRecords(ctx, block.deviceID, cids)
	if err != nil {
		log.Errorf("AnnounceBlocksWasDelete, delete block error:%v", err)
		return failedResults, err
	}

	for _, cid := range cids {
		_, ok := result[cid]
		if ok {
			continue
		}

		err = block.blockStore.Delete(cid)
		if err != nil {
			result[cid] = err.Error()
		}
	}

	for k, v := range result {
		log.Errorf("AnnounceBlocksWasDelete, delete block %s error:%v", k, v)
		result := api.BlockOperationResult{Cid: k, ErrMsg: v}
		failedResults = append(failedResults, result)
	}

	return failedResults, nil
}

func (block *Block) QueryCacheStat(ctx context.Context) (api.CacheStat, error) {
	result := api.CacheStat{}

	keyCount, err := block.blockStore.KeyCount()
	if err != nil {
		log.Errorf("block store key count error:%v", err)
	}

	result.CacheBlockCount = keyCount
	result.WaitCacheBlockNum = len(block.reqList)
	result.DoingCacheBlockNum = len(block.cachingList)

	log.Infof("CacheBlockCount:%d,WaitCacheBlockNum:%d, DoingCacheBlockNum:%d", result.CacheBlockCount, result.WaitCacheBlockNum, result.DoingCacheBlockNum)
	return result, nil
}

func (block *Block) BlockStoreStat(ctx context.Context) error {
	log.Debug("BlockStoreStat")

	return nil
}

func (block *Block) QueryCachingBlocks(ctx context.Context) (api.CachingBlockList, error) {
	result := api.CachingBlockList{}
	return result, nil
}

func (block *Block) LoadBlock(ctx context.Context, cid string) ([]byte, error) {
	// log.Infof("LoadBlock, cid:%s", cid)
	if block.blockStore == nil {
		log.Errorf("LoadData, blockStore not setting")
		return nil, nil
	}

	return block.blockStore.Get(cid)
}

func (block *Block) GetAllCidsFromBlockStore() ([]string, error) {
	return block.blockStore.GetAllKeys()
}

func (block *Block) LoadBlockWithFid(fid string) ([]byte, error) {
	cid, err := block.getCID(fid)
	if err != nil {
		return nil, err
	}

	return block.blockStore.Get(cid)
}

func (block *Block) getCID(fid string) (string, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	value, err := block.ds.Get(ctx, helper.NewKeyFID(fid))
	if err != nil {
		// log.Errorf("Get cid from store error:%v, fid:%s", err, fid)
		return "", err
	}

	return string(value), nil
}

func (block *Block) getFID(cid string) (string, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	value, err := block.ds.Get(ctx, helper.NewKeyCID(cid))
	if err != nil {
		// log.Errorf("Get fid from store error:%v, cid:%s", err, cid)
		return "", err
	}

	return string(value), nil
}
