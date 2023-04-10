package asset

import (
	"context"
	"fmt"

	"github.com/ipfs/go-cid"
	legacy "github.com/ipfs/go-ipld-legacy"
	"github.com/linguohua/titan/api/types"
	"github.com/linguohua/titan/node/asset/fetcher"
	"github.com/linguohua/titan/node/asset/storage"
)

type pulledResult struct {
	netLayerCids []string
	linksSize    uint64
	doneSize     uint64
}

type assetPuller struct {
	root            cid.Cid
	storage         storage.Storage
	bFetcher        fetcher.BlockFetcher
	downloadSources []*types.CandidateDownloadInfo

	blocksWaitList          []string
	blocksPulledSuccessList []string
	// nextLayerCIDs just for restore pull task
	nextLayerCIDs []string
	totalSize     uint64
	doneSize      uint64
	// pull block async
	parallel int
	isFinish bool
}

type pullerOptions struct {
	root     cid.Cid
	dss      []*types.CandidateDownloadInfo
	storage  storage.Storage
	bFetcher fetcher.BlockFetcher
	parallel int
}

func newAssetPuller(opts *pullerOptions) *assetPuller {
	return &assetPuller{root: opts.root, storage: opts.storage, downloadSources: opts.dss, bFetcher: opts.bFetcher, parallel: opts.parallel}
}

// get n block from front of wait list
func (ap *assetPuller) getBlocksFromWaitListFront(n int) []string {
	if len(ap.blocksWaitList) < n {
		n = len(ap.blocksWaitList)
	}

	return ap.blocksWaitList[:n]
}

// remove n block from front of wait list
func (ap *assetPuller) removeBlocksFromWaitList(n int) {
	if len(ap.blocksWaitList) < n {
		n = len(ap.blocksWaitList)
	}
	ap.blocksWaitList = ap.blocksWaitList[n:]
}

func (ap *assetPuller) pullAsset() error {
	defer func() {
		ap.isFinish = true
	}()

	netLayerCIDs := ap.blocksWaitList
	if len(netLayerCIDs) == 0 {
		netLayerCIDs = append(netLayerCIDs, ap.root.String())
	}

	for len(netLayerCIDs) > 0 {
		ret, err := ap.pullBlocksWithBreadthFirst(netLayerCIDs)
		if err != nil {
			return err
		}

		if ap.totalSize == 0 {
			ap.totalSize = ret.linksSize + ret.doneSize
		}

		netLayerCIDs = ret.netLayerCids
	}
	return nil
}

func (ap *assetPuller) pullBlocksWithBreadthFirst(layerCids []string) (result *pulledResult, err error) {
	ap.blocksWaitList = layerCids
	result = &pulledResult{netLayerCids: ap.nextLayerCIDs}
	for len(ap.blocksWaitList) > 0 {
		doLen := len(ap.blocksWaitList)
		if doLen > ap.parallel {
			doLen = ap.parallel
		}

		blocks := ap.getBlocksFromWaitListFront(doLen)
		ret, err := ap.pullBlocks(blocks)
		if err != nil {
			return nil, err
		}

		result.linksSize += ret.linksSize
		result.doneSize += ret.doneSize
		result.netLayerCids = append(result.netLayerCids, ret.netLayerCids...)

		ap.doneSize += ret.doneSize
		ap.blocksPulledSuccessList = append(ap.blocksPulledSuccessList, blocks...)
		ap.nextLayerCIDs = append(ap.nextLayerCIDs, ret.netLayerCids...)
		ap.removeBlocksFromWaitList(doLen)

	}
	ap.nextLayerCIDs = make([]string, 0)

	return result, nil
}

func (ap *assetPuller) pullBlocks(cids []string) (*pulledResult, error) {
	blks, err := ap.bFetcher.Fetch(context.Background(), cids, ap.downloadSources)
	if err != nil {
		log.Errorf("loadBlocksAsync loadBlocks err %s", err.Error())
		return nil, err
	}

	if len(blks) != len(cids) {
		return nil, fmt.Errorf("pull blocks failed, already pull blocks len:%d, need blocks len:%v", len(blks), len(cids))
	}

	linksSize := uint64(0)
	doneSize := uint64(0)
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

		doneSize += uint64(len(b.RawData()))
		linksMap[b.Cid().String()] = subCIDS
	}

	nexLayerCids := make([]string, 0)
	for _, cid := range cids {
		links := linksMap[cid]
		nexLayerCids = append(nexLayerCids, links...)
	}

	err = ap.storage.PutBlocks(context.Background(), ap.root, blks)
	if err != nil {
		return nil, err
	}

	ret := &pulledResult{netLayerCids: nexLayerCids, linksSize: linksSize, doneSize: doneSize}

	return ret, nil
}

func (ap *assetPuller) isPulledComplete() bool {
	if ap.totalSize == 0 {
		return false
	}

	if ap.doneSize != ap.totalSize {
		return false
	}

	return true
}

func (ap *assetPuller) cancelPulling() error {
	// TODO: implement cancel
	return fmt.Errorf("")
}

func (ap *assetPuller) encode() ([]byte, error) {
	eac := &EncodeAssetPuller{
		Root:                    ap.root.String(),
		BlocksWaitList:          ap.blocksWaitList,
		BlocksPulledSuccessList: ap.blocksPulledSuccessList,
		NextLayerCIDs:           ap.nextLayerCIDs,
		DownloadSources:         ap.downloadSources,
		TotalSize:               ap.totalSize,
		DoneSize:                ap.doneSize,
	}

	return encode(eac)
}

func (ap *assetPuller) decode(data []byte) error {
	eac := &EncodeAssetPuller{}
	err := decode(data, eac)
	if err != nil {
		return err
	}

	c, err := cid.Decode(eac.Root)
	if err != nil {
		return err
	}

	ap.root = c
	ap.blocksWaitList = eac.BlocksWaitList
	ap.blocksPulledSuccessList = eac.BlocksPulledSuccessList
	ap.nextLayerCIDs = eac.NextLayerCIDs
	ap.downloadSources = eac.DownloadSources
	ap.totalSize = eac.TotalSize
	ap.doneSize = eac.DoneSize

	return nil
}

func (ap *assetPuller) status() types.ReplicaStatus {
	if ap.isPulledComplete() {
		return types.ReplicaStatusSucceeded
	}

	if ap.isFinish {
		return types.ReplicaStatusFailed
	}
	return types.ReplicaStatusPulling
}

func (ap *assetPuller) progress() *types.AssetPullProgress {
	return &types.AssetPullProgress{
		CID:             ap.root.String(),
		Status:          ap.status(),
		BlocksCount:     len(ap.blocksPulledSuccessList) + len(ap.blocksWaitList),
		DoneBlocksCount: len(ap.blocksPulledSuccessList),
		Size:            int64(ap.totalSize),
		DoneSize:        int64(ap.doneSize),
	}
}
