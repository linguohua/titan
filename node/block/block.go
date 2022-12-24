package block

import (
	"container/list"
	"context"
	"fmt"
	"sync"
	"time"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	exchange "github.com/ipfs/go-ipfs-exchange-interface"
	"github.com/ipfs/go-merkledag"
	dagpb "github.com/ipld/go-codec-dagpb"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/blockstore"
	"github.com/linguohua/titan/node/helper"

	format "github.com/ipfs/go-ipld-format"
	legacy "github.com/ipfs/go-ipld-legacy"
	logging "github.com/ipfs/go-log/v2"
)

var log = logging.Logger("block")

type delayReq struct {
	blockInfo api.BlockCacheInfo
	count     int
	// use for edge node load block
	downloadURL   string
	downloadToken string
	carFileHash   string
	CacheID       string
}

type blockStat struct {
	cid         string
	links       []string
	blockSize   int
	linksSize   uint64
	carFileHash string
	CacheID     string
}

type Block struct {
	ds         datastore.Batching
	blockStore blockstore.BlockStore
	scheduler  api.Scheduler
	// carfile block list
	carfileList *list.List
	// key is carfile hash
	carfileMap    map[string]*list.Element
	cachingList   []*delayReq
	saveBlockLock *sync.Mutex
	blockLoader   BlockLoader
	deviceID      string
	exchange      exchange.Interface
	blockLoaderCh chan bool
	ipfsGateway   string
}

type BlockLoader interface {
	// scheduler request cache carfile
	loadBlocks(block *Block, req []*delayReq)
	// local sync miss data
	syncData(block *Block, reqs map[int]string) error
}

func NewBlock(ds datastore.Batching, blockStore blockstore.BlockStore, scheduler api.Scheduler, blockLoader BlockLoader, ipfsGateway, deviceID string) *Block {
	block := &Block{
		ds:          ds,
		blockStore:  blockStore,
		scheduler:   scheduler,
		blockLoader: blockLoader,
		exchange:    nil,
		deviceID:    deviceID,

		saveBlockLock: &sync.Mutex{},

		blockLoaderCh: make(chan bool),
		ipfsGateway:   ipfsGateway,
		carfileList:   list.New(),
		carfileMap:    make(map[string]*list.Element),
	}

	go block.startBlockLoader()

	legacy.RegisterCodec(cid.DagProtobuf, dagpb.Type.PBNode, merkledag.ProtoNodeConverter)
	legacy.RegisterCodec(cid.Raw, basicnode.Prototype.Bytes, merkledag.RawNodeConverter)

	return block
}

func apiReq2DelayReq(req *api.ReqCacheData) []*delayReq {
	results := make([]*delayReq, 0, len(req.BlockInfos))
	for _, blockInfo := range req.BlockInfos {
		if len(blockInfo.Cid) == 0 {
			continue
		}

		req := &delayReq{blockInfo: blockInfo, count: 0, downloadURL: req.DownloadURL, downloadToken: req.DownloadToken, carFileHash: req.CardFileHash, CacheID: req.CacheID}
		results = append(results, req)
	}

	return results
}

func (block *Block) startBlockLoader() {
	if block.blockLoader == nil {
		log.Panic("block.block == nil")
	}

	for {
		<-block.blockLoaderCh
		block.doLoadBlock()
	}
}

func (block *Block) notifyBlockLoader() {
	select {
	case block.blockLoaderCh <- true:
	default:
	}
}

func (block *Block) doLoadBlock() {
	element := block.carfileList.Back()
	if element == nil {
		return
	}

	carfile, ok := element.Value.(*carfile)
	if !ok {
		log.Panicf("doLoadBlock error, can convert elemnet to carfile")
	}

	for len(carfile.delayReqs) > 0 {
		doLen := len(carfile.delayReqs)
		if doLen > helper.Batch {
			doLen = helper.Batch
		}

		doReqs := carfile.removeReq(doLen)
		block.cachingList = doReqs

		block.blockLoader.loadBlocks(block, doReqs)
		block.cachingList = nil
	}

	block.carfileList.Remove(element)
	delete(block.carfileMap, carfile.carfileHash)
}

func (block *Block) getWaitCacheBlockNum() int {
	count := 0
	for e := block.carfileList.Front(); e != nil; e = e.Next() {
		carfile := e.Value.(*carfile)
		count += len(carfile.delayReqs)
	}
	return count
}

func (block *Block) addReq2WaitList(req *api.ReqCacheData) {
	element, ok := block.carfileMap[req.CardFileHash]
	if !ok {
		cf := &carfile{carfileHash: req.CardFileHash, lock: &sync.Mutex{}, delayReqs: make([]*delayReq, 0)}
		element = block.carfileList.PushBack(cf)
		block.carfileMap[req.CardFileHash] = element
	}

	carfile, ok := element.Value.(*carfile)
	if !ok {
		log.Panicf("addReq2WaitList error, can convert elemnet to carfile")
	}

	carfile.addReq(apiReq2DelayReq(req))

	block.notifyBlockLoader()
}

func (block *Block) cacheResultWithError(ctx context.Context, bStat blockStat, err error) {
	log.Errorf("cacheResultWithError, cid:%s, cacheID:%s, carFileHash:%s, error:%v", bStat.cid, bStat.CacheID, bStat.carFileHash, err)
	block.cacheResult(ctx, err, bStat)
}

func (block *Block) cacheResult(ctx context.Context, err error, bStat blockStat) {
	errMsg := ""
	success := true
	if err != nil {
		success = false
		errMsg = err.Error()
	}

	result := api.CacheResultInfo{
		Cid:         bStat.cid,
		IsOK:        success,
		Msg:         errMsg,
		From:        "",
		Links:       bStat.links,
		BlockSize:   bStat.blockSize,
		LinksSize:   bStat.linksSize,
		CarFileHash: bStat.carFileHash,
		CacheID:     bStat.CacheID,
	}

	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	_, err = block.scheduler.CacheResult(ctx, block.deviceID, result)
	if err != nil {
		log.Errorf("cacheResult CacheResult error:%v", err)
		return
	}
}

func (block *Block) filterAvailableReq(reqs []*delayReq) []*delayReq {
	ctx := context.Background()
	results := make([]*delayReq, 0, len(reqs))
	for _, reqData := range reqs {
		cid, err := cid.Decode(reqData.blockInfo.Cid)
		if err != nil {
			continue
		}

		buf, err := block.getBlockWithCID(cid.String())
		if err == nil {
			newFid := fmt.Sprintf("%d", reqData.blockInfo.Fid)
			oldFid, _ := block.getFIDFromCID(reqData.blockInfo.Cid)
			if oldFid != newFid {
				block.updateCidAndFid(ctx, cid, newFid)
			}

			links, err := getLinks(block, buf, cid.String())
			if err != nil {
				log.Errorf("filterAvailableReq getLinks error:%s", err.Error())
				continue
			}

			linksSize := uint64(0)
			cids := make([]string, 0, len(links))
			for _, link := range links {
				cids = append(cids, link.Cid.String())
				linksSize += link.Size
			}

			bStat := blockStat{cid: cid.String(), links: cids, blockSize: len(buf), linksSize: linksSize, carFileHash: reqData.carFileHash, CacheID: reqData.CacheID}
			block.cacheResult(ctx, nil, bStat)
			continue
		}

		results = append(results, reqData)
	}

	return results
}

func (block *Block) CacheBlocks(ctx context.Context, reqs []api.ReqCacheData) (api.CacheStat, error) {
	log.Infof("CacheBlocks, reqs:%d", len(reqs))
	for _, req := range reqs {
		reqCacheData := req
		block.addReq2WaitList(&reqCacheData)
	}

	return block.QueryCacheStat(ctx)
}

func (block *Block) RemoveWaitCacheBlockWith(ctx context.Context, carfileCID string) error {
	carfileHash, err := helper.CIDString2HashString(carfileCID)
	if err != nil {
		return err
	}

	element, ok := block.carfileMap[carfileHash]
	if !ok {
		log.Errorf("RemoveWaitingBlockWithCarfileCID, no carfile %s block wait cache", carfileCID)
		return nil
	}

	carfile, ok := element.Value.(*carfile)
	if !ok {
		log.Panicf("addReq2WaitList error, can convert elemnet to carfile")
	}

	carfile.delayReqs = nil

	delete(block.carfileMap, carfileHash)
	block.carfileList.Remove(element)

	return nil
}

// delete block in local store and scheduler
func (block *Block) DeleteBlocks(ctx context.Context, cids []string) ([]api.BlockOperationResult, error) {
	log.Infof("DeleteBlocks, cids len:%d", len(cids))
	// delResult := api.DelResult{}
	results := make([]api.BlockOperationResult, 0)

	if block.blockStore == nil {
		log.Errorf("DeleteBlocks, blockStore not setting")
		return results, fmt.Errorf("edge.blockStore == nil")
	}

	for _, cid := range cids {
		err := block.deleteBlock(cid)
		if err == datastore.ErrNotFound {
			log.Infof("DeleteBlocks cid %s not exist", cid)
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

		err = block.deleteBlock(cid)
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
	result.WaitCacheBlockNum = block.getWaitCacheBlockNum()
	result.DoingCacheBlockNum = len(block.cachingList)
	result.RetryNum = helper.BlockDownloadRetryNum
	result.DownloadTimeout = helper.BlockDownloadTimeout

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

	return block.getBlockWithCID(cid)
}

func (block *Block) GetAllCidsFromBlockStore() ([]string, error) {
	return block.blockStore.GetAllKeys()
}

func (block *Block) DeleteAllBlocks(ctx context.Context) error {
	return block.deleteAllBlocks()
}

func (block *Block) GetCID(ctx context.Context, fid string) (string, error) {
	cid, err := block.getCIDFromFID(fid)
	if err != nil {
		return "", err
	}
	return cid.String(), nil
}

func (block *Block) GetFID(ctx context.Context, cid string) (string, error) {
	return block.getFIDFromCID(cid)
}

func (block *Block) LoadBlockWithFid(fid string) ([]byte, error) {
	return block.getBlockWithFID(fid)
}

func (block *Block) GetDatastore(ctx context.Context) datastore.Batching {
	return block.ds
}

func (block *Block) resolveLinks(blk blocks.Block) ([]*format.Link, error) {
	ctx := context.Background()

	node, err := legacy.DecodeNode(ctx, blk)
	if err != nil {
		log.Error("resolveLinks err:%v", err)
		return make([]*format.Link, 0), err
	}

	return node.Links(), nil
}

func (block *Block) SyncData(reqs map[int]string) error {
	return block.blockLoader.syncData(block, reqs)
}

func getLinks(block *Block, data []byte, cidStr string) ([]*format.Link, error) {
	if len(data) == 0 {
		return make([]*format.Link, 0), nil
	}

	target, err := cid.Decode(cidStr)
	if err != nil {
		return make([]*format.Link, 0), err
	}

	blk, err := blocks.NewBlockWithCid(data, target)
	if err != nil {
		return make([]*format.Link, 0), err
	}

	return block.resolveLinks(blk)
}
