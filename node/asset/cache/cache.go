package cache

import (
	"context"
	"fmt"

	"github.com/ipfs/go-cid"
	legacy "github.com/ipfs/go-ipld-legacy"
	logging "github.com/ipfs/go-log/v2"
	"github.com/linguohua/titan/api/types"
	"github.com/linguohua/titan/node/asset/fetcher"
	"github.com/linguohua/titan/node/asset/storage"
)

var log = logging.Logger("asset/cache")

type downloadResult struct {
	netLayerCids []string
	linksSize    uint64
	downloadSize uint64
}

type assetCache struct {
	root            cid.Cid
	storage         storage.Storage
	bFetcher        fetcher.BlockFetcher
	downloadSources []*types.CandidateDownloadInfo

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
	dss      []*types.CandidateDownloadInfo
	storage  storage.Storage
	bFetcher fetcher.BlockFetcher
	batch    int
}

func newAssetCache(opts *options) *assetCache {
	return &assetCache{root: opts.root, storage: opts.storage, downloadSources: opts.dss, bFetcher: opts.bFetcher, batch: opts.batch}
}

// get n block from front of wait list
func (ac *assetCache) getBlocksFromWaitListFront(n int) []string {
	if len(ac.blocksWaitList) < n {
		n = len(ac.blocksWaitList)
	}

	return ac.blocksWaitList[:n]
}

// remove n block from front of wait list
func (ac *assetCache) removeBlocksFromWaitList(n int) {
	if len(ac.blocksWaitList) < n {
		n = len(ac.blocksWaitList)
	}
	ac.blocksWaitList = ac.blocksWaitList[n:]
}

func (ac *assetCache) downloadAsset() error {
	defer func() {
		ac.isFinish = true
	}()

	netLayerCIDs := ac.blocksWaitList
	if len(netLayerCIDs) == 0 {
		netLayerCIDs = append(netLayerCIDs, ac.root.String())
	}

	for len(netLayerCIDs) > 0 {
		ret, err := ac.downloadBlocksWithBreadthFirst(netLayerCIDs)
		if err != nil {
			return err
		}

		if ac.totalSize == 0 {
			ac.totalSize = ret.linksSize + ret.downloadSize
		}

		netLayerCIDs = ret.netLayerCids
	}
	return nil
}

func (ac *assetCache) downloadBlocksWithBreadthFirst(layerCids []string) (result *downloadResult, err error) {
	ac.blocksWaitList = layerCids
	result = &downloadResult{netLayerCids: ac.nextLayerCIDs}
	for len(ac.blocksWaitList) > 0 {
		doLen := len(ac.blocksWaitList)
		if doLen > ac.batch {
			doLen = ac.batch
		}

		blocks := ac.getBlocksFromWaitListFront(doLen)
		ret, err := ac.downloadBlocks(blocks)
		if err != nil {
			return nil, err
		}

		result.linksSize += ret.linksSize
		result.downloadSize += ret.downloadSize
		result.netLayerCids = append(result.netLayerCids, ret.netLayerCids...)

		ac.doneSize += ret.downloadSize
		ac.blocksDownloadSuccessList = append(ac.blocksDownloadSuccessList, blocks...)
		ac.nextLayerCIDs = append(ac.nextLayerCIDs, ret.netLayerCids...)
		ac.removeBlocksFromWaitList(doLen)

	}
	ac.nextLayerCIDs = make([]string, 0)

	return result, nil
}

func (ac *assetCache) downloadBlocks(cids []string) (*downloadResult, error) {
	blks, err := ac.bFetcher.Fetch(context.Background(), cids, ac.downloadSources)
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

	err = ac.storage.PutBlocks(context.Background(), ac.root, blks)
	if err != nil {
		return nil, err
	}

	ret := &downloadResult{netLayerCids: nexLayerCids, linksSize: linksSize, downloadSize: downloadSize}

	return ret, nil
}

func (ac *assetCache) isDownloadComplete() bool {
	if ac.totalSize == 0 {
		return false
	}

	if ac.doneSize != ac.totalSize {
		return false
	}

	return true
}

func (ac *assetCache) Root() cid.Cid {
	return ac.root
}

func (ac *assetCache) DoneSize() int64 {
	return int64(ac.doneSize)
}

func (ac *assetCache) TotalSize() int64 {
	return int64(ac.totalSize)
}

func (ac *assetCache) CancelDownload() error {
	// TODO: implement cancel
	return fmt.Errorf("")
}

func (ac *assetCache) encode() ([]byte, error) {
	eac := &EncodeAssetCache{
		Root:                      ac.root.String(),
		BlocksWaitList:            ac.blocksWaitList,
		BlocksDownloadSuccessList: ac.blocksDownloadSuccessList,
		NextLayerCIDs:             ac.nextLayerCIDs,
		DownloadSources:           ac.downloadSources,
		TotalSize:                 ac.totalSize,
		DoneSize:                  ac.doneSize,
	}

	return encode(eac)
}

func (ac *assetCache) decode(data []byte) error {
	eac := &EncodeAssetCache{}
	err := decode(data, eac)
	if err != nil {
		return err
	}

	c, err := cid.Decode(eac.Root)
	if err != nil {
		return err
	}

	ac.root = c
	ac.blocksWaitList = eac.BlocksWaitList
	ac.blocksDownloadSuccessList = eac.BlocksDownloadSuccessList
	ac.nextLayerCIDs = eac.NextLayerCIDs
	ac.downloadSources = eac.DownloadSources
	ac.totalSize = eac.TotalSize
	ac.doneSize = eac.DoneSize

	return nil
}

func (ac *assetCache) cacheStatus() types.ReplicaStatus {
	if ac.isDownloadComplete() {
		return types.ReplicaStatusSucceeded
	}

	if ac.isFinish {
		return types.ReplicaStatusFailed
	}
	return types.ReplicaStatusPulling
}

func (ac *assetCache) Progress() *types.AssetPullProgress {
	return &types.AssetPullProgress{
		CID:             ac.root.String(),
		Status:          ac.cacheStatus(),
		BlocksCount:     len(ac.blocksDownloadSuccessList) + len(ac.blocksWaitList),
		DoneBlocksCount: len(ac.blocksDownloadSuccessList),
		Size:            ac.TotalSize(),
		DoneSize:        ac.DoneSize(),
	}
}
