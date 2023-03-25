package cache

import (
	"context"
	"fmt"

	"github.com/ipfs/go-cid"
	legacy "github.com/ipfs/go-ipld-legacy"
	logging "github.com/ipfs/go-log/v2"
	"github.com/linguohua/titan/api/types"
	"github.com/linguohua/titan/node/carfile/fetcher"
	"github.com/linguohua/titan/node/carfile/store"
)

var log = logging.Logger("carfile/cache")

type downloadResult struct {
	netLayerCids []string
	linksSize    uint64
	downloadSize uint64
}

type carfileCache struct {
	root            cid.Cid
	cs              *store.CarfileStore
	bFetcher        fetcher.BlockFetcher
	downloadSources []*types.DownloadSource

	blocksWaitList            []string
	blocksDownloadSuccessList []string
	// nextLayerCIDs just for restore download task
	nextLayerCIDs []string
	totalSize     uint64
	doneSize      uint64
	// download block async
	batch    int
	isFinish bool
}

type options struct {
	root     cid.Cid
	dss      []*types.DownloadSource
	cs       *store.CarfileStore
	bFetcher fetcher.BlockFetcher
	batch    int
}

func newCarfileCache(opts *options) *carfileCache {
	return &carfileCache{root: opts.root, cs: opts.cs, downloadSources: opts.dss, bFetcher: opts.bFetcher, batch: opts.batch}
}

// get n block from front of wait list
func (cfCache *carfileCache) getBlocksFromWaitListFront(n int) []string {
	if len(cfCache.blocksWaitList) < n {
		n = len(cfCache.blocksWaitList)
	}

	return cfCache.blocksWaitList[:n]
}

// remove n block from front of wait list
func (cfCache *carfileCache) removeBlocksFromWaitList(n int) {
	if len(cfCache.blocksWaitList) < n {
		n = len(cfCache.blocksWaitList)
	}
	cfCache.blocksWaitList = cfCache.blocksWaitList[n:]
}

func (cfCache *carfileCache) downloadCar() error {
	defer func() {
		cfCache.isFinish = true
	}()

	netLayerCIDs := cfCache.blocksWaitList
	if len(netLayerCIDs) == 0 {
		netLayerCIDs = append(netLayerCIDs, cfCache.root.String())
	}

	for len(netLayerCIDs) > 0 {
		ret, err := cfCache.downloadBlocksWithBreadthFirst(netLayerCIDs)
		if err != nil {
			return err
		}

		if cfCache.totalSize == 0 {
			cfCache.totalSize = ret.linksSize + ret.downloadSize
		}

		netLayerCIDs = ret.netLayerCids
	}
	return nil
}

func (cfCache *carfileCache) downloadBlocksWithBreadthFirst(layerCids []string) (result *downloadResult, err error) {
	cfCache.blocksWaitList = layerCids
	result = &downloadResult{netLayerCids: cfCache.nextLayerCIDs}
	for len(cfCache.blocksWaitList) > 0 {
		doLen := len(cfCache.blocksWaitList)
		if doLen > cfCache.batch {
			doLen = cfCache.batch
		}

		blocks := cfCache.getBlocksFromWaitListFront(doLen)
		ret, err := cfCache.downloadBlocks(blocks)
		if err != nil {
			return nil, err
		}

		result.linksSize += ret.linksSize
		result.downloadSize += ret.downloadSize
		result.netLayerCids = append(result.netLayerCids, ret.netLayerCids...)

		cfCache.doneSize += ret.downloadSize
		cfCache.blocksDownloadSuccessList = append(cfCache.blocksDownloadSuccessList, blocks...)
		cfCache.nextLayerCIDs = append(cfCache.nextLayerCIDs, ret.netLayerCids...)
		cfCache.removeBlocksFromWaitList(doLen)
	}
	cfCache.nextLayerCIDs = make([]string, 0)

	return result, nil
}

func (cfCache *carfileCache) downloadBlocks(cids []string) (*downloadResult, error) {
	blks, err := cfCache.bFetcher.Fetch(context.Background(), cids, cfCache.downloadSources)
	if err != nil {
		log.Errorf("loadBlocksAsync loadBlocks err %s", err.Error())
		return nil, err
	}

	if len(blks) != len(cids) {
		return nil, fmt.Errorf("download blocks failed, blks len:%d, cids len:%v", len(blks), len(cids))
	}

	linksSize := uint64(0)
	downloadSize := uint64(0)
	linksMap := make(map[string][]string)
	for _, b := range blks {
		// get block links
		node, err := legacy.DecodeNode(context.Background(), b)
		if err != nil {
			log.Errorf("downloadBlocks decode block error:%s", err.Error())
			return nil, err
		}

		links := node.Links()
		subCIDS := make([]string, 0, len(links))
		for _, link := range links {
			subCIDS = append(subCIDS, link.Cid.String())
			linksSize += link.Size
		}

		downloadSize += uint64(len(b.RawData()))
		linksMap[b.Cid().String()] = subCIDS
	}

	nexLayerCids := make([]string, 0)
	for _, cid := range cids {
		links := linksMap[cid]
		nexLayerCids = append(nexLayerCids, links...)
	}

	err = cfCache.cs.PutBlocks(context.Background(), cfCache.root, blks)
	if err != nil {
		return nil, err
	}

	ret := &downloadResult{netLayerCids: nexLayerCids, linksSize: linksSize, downloadSize: downloadSize}

	return ret, nil
}

func (cfCache *carfileCache) isDownloadComplete() bool {
	if cfCache.totalSize == 0 {
		return false
	}

	if cfCache.doneSize != cfCache.totalSize {
		return false
	}

	return true
}

func (cfCache *carfileCache) Root() cid.Cid {
	return cfCache.root
}

func (cfCache *carfileCache) DoneSize() int64 {
	return int64(cfCache.doneSize)
}

func (cfCache *carfileCache) TotalSize() int64 {
	return int64(cfCache.totalSize)
}

func (cfCache *carfileCache) CancelDownload() error {
	// TODO: implement cancel
	return fmt.Errorf("")
}

func (cfCache *carfileCache) encode() ([]byte, error) {
	encodeCarfile := &EncodeCarfileCache{
		Root:                      cfCache.root.String(),
		BlocksWaitList:            cfCache.blocksWaitList,
		BlocksDownloadSuccessList: cfCache.blocksDownloadSuccessList,
		NextLayerCIDs:             cfCache.nextLayerCIDs,
		DownloadSources:           cfCache.downloadSources,
		TotalSize:                 cfCache.totalSize,
		DoneSize:                  cfCache.doneSize,
	}

	return encode(encodeCarfile)
}

func (cfCache *carfileCache) decode(data []byte) error {
	encodeCarfile := &EncodeCarfileCache{}
	err := decode(data, encodeCarfile)
	if err != nil {
		return err
	}

	c, err := cid.Decode(encodeCarfile.Root)
	if err != nil {
		return err
	}

	cfCache.root = c
	cfCache.blocksWaitList = encodeCarfile.BlocksWaitList
	cfCache.blocksDownloadSuccessList = encodeCarfile.BlocksDownloadSuccessList
	cfCache.nextLayerCIDs = encodeCarfile.NextLayerCIDs
	cfCache.downloadSources = encodeCarfile.DownloadSources
	cfCache.totalSize = encodeCarfile.TotalSize
	cfCache.doneSize = encodeCarfile.DoneSize

	return nil
}

func (cfCache *carfileCache) cacheStatus() types.CacheStatus {
	if cfCache.isDownloadComplete() {
		return types.CacheStatusSucceeded
	}

	if cfCache.isFinish {
		return types.CacheStatusFailed
	}
	return types.CacheStatusCaching
}

func (cfCache *carfileCache) Progress() *types.AssetCacheProgress {
	return &types.AssetCacheProgress{
		CID:             cfCache.root.String(),
		Status:          cfCache.cacheStatus(),
		BlocksCount:     len(cfCache.blocksDownloadSuccessList) + len(cfCache.blocksWaitList),
		DoneBlocksCount: len(cfCache.blocksDownloadSuccessList),
		Size:            cfCache.TotalSize(),
		DoneSize:        cfCache.DoneSize(),
	}
}
