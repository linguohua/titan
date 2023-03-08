package carfile

import (
	"context"
	"fmt"
	"github.com/linguohua/titan/api/types"

	blocks "github.com/ipfs/go-block-format"
	format "github.com/ipfs/go-ipld-format"
	legacy "github.com/ipfs/go-ipld-legacy"
	"github.com/linguohua/titan/node/cidutil"
)

type downloadResult struct {
	netLayerCids []string
	linksSize    uint64
	downloadSize uint64
}

type carfileCache struct {
	carfileCID                string
	blocksWaitList            []string
	blocksDownloadSuccessList []string
	// nextLayerCIDs just for restore download task
	nextLayerCIDs   []string
	downloadSources []*types.DownloadSource
	carfileSize     uint64
	downloadSize    uint64
}

func resolveLinks(blk blocks.Block) ([]*format.Link, error) {
	ctx := context.Background()

	node, err := legacy.DecodeNode(ctx, blk)
	if err != nil {
		log.Error("resolveLinks err:%v", err)
		return make([]*format.Link, 0), err
	}

	return node.Links(), nil
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

func (cfCache *carfileCache) addBlocks2WaitList(blocks []string) {
	cfCache.blocksWaitList = append(cfCache.blocksWaitList, blocks...)
}

func (cfCache *carfileCache) downloadCarfile(downloadOperation DownloadOperation) error {
	netLayerCIDs := cfCache.blocksWaitList
	if len(netLayerCIDs) == 0 {
		netLayerCIDs = append(netLayerCIDs, cfCache.carfileCID)
	}

	for len(netLayerCIDs) > 0 {
		ret, err := cfCache.downloadBlocksWithBreadthFirst(netLayerCIDs, downloadOperation)
		if err != nil {
			return err
		}

		if cfCache.carfileSize == 0 {
			cfCache.carfileSize = ret.linksSize + ret.downloadSize
		}

		netLayerCIDs = ret.netLayerCids

	}
	return nil
}

func (cfCache *carfileCache) downloadBlocksWithBreadthFirst(layerCids []string, downloadOperation DownloadOperation) (result *downloadResult, err error) {
	cfCache.blocksWaitList = layerCids
	result = &downloadResult{netLayerCids: cfCache.nextLayerCIDs}
	for len(cfCache.blocksWaitList) > 0 {
		doLen := len(cfCache.blocksWaitList)
		if doLen > batch {
			doLen = batch
		}

		blocks := cfCache.getBlocksFromWaitListFront(doLen)
		ret, err := cfCache.downloadBlocks(blocks, downloadOperation)
		if err != nil {
			return nil, err
		}

		result.linksSize += ret.linksSize
		result.downloadSize += ret.downloadSize
		result.netLayerCids = append(result.netLayerCids, ret.netLayerCids...)

		cfCache.downloadSize += ret.downloadSize
		cfCache.blocksDownloadSuccessList = append(cfCache.blocksDownloadSuccessList, blocks...)
		cfCache.nextLayerCIDs = append(cfCache.nextLayerCIDs, ret.netLayerCids...)
		cfCache.removeBlocksFromWaitList(doLen)
	}
	cfCache.nextLayerCIDs = make([]string, 0)

	return result, nil
}

func (cfCache *carfileCache) downloadBlocks(cids []string, downloadOperation DownloadOperation) (*downloadResult, error) {
	blks, err := downloadOperation.downloadBlocks(cids, cfCache.downloadSources)
	if err != nil {
		log.Errorf("loadBlocksAsync loadBlocks err %s", err.Error())
		return nil, err
	}

	if len(blks) != len(cids) {
		return nil, fmt.Errorf("download blocks failed, blks len:%d, cids len:%v", len(blks), len(cids))
	}

	carfileHash, err := cfCache.getCarfileHashString()
	if err != nil {
		return nil, err
	}

	linksSize := uint64(0)
	downloadSize := uint64(0)
	linksMap := make(map[string][]string)
	for _, b := range blks {
		err = downloadOperation.saveBlock(b.RawData(), b.Cid().Hash().String(), carfileHash)
		if err != nil {
			log.Errorf("loadBlocksFromIPFS save block error:%s", err.Error())
			continue
		}

		// get block links
		links, err := resolveLinks(b)
		if err != nil {
			log.Errorf("loadBlocksFromIPFS resolveLinks error:%s", err.Error())
			continue
		}

		// linksSize := uint64(0)
		cids := make([]string, 0, len(links))
		for _, link := range links {
			cids = append(cids, link.Cid.String())
			linksSize += link.Size
		}

		downloadSize += uint64(len(b.RawData()))
		linksMap[b.Cid().String()] = cids
	}

	nexLayerCids := make([]string, 0)
	for _, cid := range cids {
		links := linksMap[cid]
		nexLayerCids = append(nexLayerCids, links...)
	}

	ret := &downloadResult{netLayerCids: nexLayerCids, linksSize: linksSize, downloadSize: downloadSize}

	return ret, nil
}

func (cfCache *carfileCache) blockList2BlocksHashString() (string, error) {
	var blocksHashString string
	for _, cid := range cfCache.blocksDownloadSuccessList {
		blockHash, err := cidutil.CIDString2HashString(cid)
		if err != nil {
			return "", err
		}
		blocksHashString += blockHash
	}

	return blocksHashString, nil
}

func (cfCache *carfileCache) blockCidList2BlocksHashList() ([]string, error) {
	blocksHashList := make([]string, 0, len(cfCache.blocksDownloadSuccessList))
	for _, cid := range cfCache.blocksDownloadSuccessList {
		blockHash, err := cidutil.CIDString2HashString(cid)
		if err != nil {
			return nil, err
		}
		blocksHashList = append(blocksHashList, blockHash)
	}

	return blocksHashList, nil
}

func (cfCache *carfileCache) getCarfileHashString() (string, error) {
	carfileHash, err := cidutil.CIDString2HashString(cfCache.carfileCID)
	if err != nil {
		return "", err
	}
	return carfileHash, nil
}
