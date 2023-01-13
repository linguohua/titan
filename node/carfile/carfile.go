package carfile

import (
	"fmt"

	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/node/helper"
)

// type downloadSource struct {
// 	downloadURL   string
// 	downloadToken string
// }

type downloadResult struct {
	netLayerCids []string
	linksSize    uint64
	downloadSize uint64
}

type carfile struct {
	carfileCID                string
	blocksWaitList            []string
	blocksDownloadSuccessList []string
	// netLayerCIDs just for restore download task
	nextLayerCIDs   []string
	downloadSources []*api.DowloadSource
	carfileSize     uint64
	downloadSize    uint64
}

func newCarfileCache(carfileCID string, downloadSources []*api.DowloadSource) *carfile {
	return &carfile{
		carfileCID:                carfileCID,
		blocksWaitList:            make([]string, 0),
		blocksDownloadSuccessList: make([]string, 0),
		nextLayerCIDs:             make([]string, 0),
		downloadSources:           downloadSources}
}

func (cf *carfile) getBlocksFromWaitListFront(n int) []string {
	if len(cf.blocksWaitList) < n {
		n = len(cf.blocksWaitList)
	}

	return cf.blocksWaitList[:n]
}

func (cf *carfile) removeBlocksFromWaitList(n int) {
	if len(cf.blocksWaitList) < n {
		n = len(cf.blocksWaitList)
	}
	cf.blocksWaitList = cf.blocksWaitList[n:]
}

func (cf *carfile) addBlocks2WaitList(blocks []string) {
	cf.blocksWaitList = append(cf.blocksWaitList, blocks...)
}

func (cf *carfile) downloadCarfile(carfileOperation *CarfileOperation) error {
	netLayerCIDs := cf.blocksWaitList
	if len(netLayerCIDs) == 0 {
		netLayerCIDs = append(netLayerCIDs, cf.carfileCID)
	}

	for len(netLayerCIDs) > 0 {
		ret, err := cf.downloadBlocksWithBreadthFirst(netLayerCIDs, carfileOperation)
		if err != nil {
			return err
		}

		if cf.carfileSize == 0 {
			cf.carfileSize = ret.linksSize + ret.downloadSize
		}

		netLayerCIDs = ret.netLayerCids

	}
	return nil
}

func (cf *carfile) downloadBlocksWithBreadthFirst(layerCids []string, cfOperation *CarfileOperation) (result *downloadResult, err error) {
	cf.blocksWaitList = layerCids
	result = &downloadResult{netLayerCids: cf.nextLayerCIDs}
	for len(cf.blocksWaitList) > 0 {
		doLen := len(cf.blocksWaitList)
		if doLen > helper.Batch {
			doLen = helper.Batch
		}

		blocks := cf.getBlocksFromWaitListFront(doLen)
		ret, err := cf.downloadBlocks(blocks, cfOperation)
		if err != nil {
			return nil, err
		}

		result.linksSize += ret.linksSize
		result.downloadSize += ret.downloadSize
		result.netLayerCids = append(result.netLayerCids, ret.netLayerCids...)

		cf.downloadSize += ret.downloadSize
		cf.blocksDownloadSuccessList = append(cf.blocksDownloadSuccessList, blocks...)
		cf.nextLayerCIDs = append(cf.nextLayerCIDs, ret.netLayerCids...)
		cf.removeBlocksFromWaitList(doLen)
	}
	cf.nextLayerCIDs = make([]string, 0)

	return result, nil
}

func (cf *carfile) downloadBlocks(cids []string, cfOperation *CarfileOperation) (*downloadResult, error) {
	// log.Infof("downloadBlocks cids:%v", cids)
	blks, err := cfOperation.downloader.downloadBlocks(cids, cf.downloadSources)
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
		cidStr := b.Cid().String()
		err = cfOperation.saveBlock(b.RawData(), cidStr)
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
		linksMap[cidStr] = cids
	}

	nexLayerCids := make([]string, 0)
	for _, cid := range cids {
		links := linksMap[cid]
		nexLayerCids = append(nexLayerCids, links...)
	}

	ret := &downloadResult{netLayerCids: nexLayerCids, linksSize: linksSize, downloadSize: downloadSize}

	return ret, nil
}

func (cf *carfile) blockList2BlocksHashString() (string, error) {
	var blocksHashString string
	for _, cid := range cf.blocksDownloadSuccessList {
		blockHash, err := helper.CIDString2HashString(cid)
		if err != nil {
			return "", err
		}
		blocksHashString += blockHash
	}

	return blocksHashString, nil
}

func (cf *carfile) blockCidList2BlocksHashList() ([]string, error) {
	blocksHashList := make([]string, 0, len(cf.blocksDownloadSuccessList))
	for _, cid := range cf.blocksDownloadSuccessList {
		blockHash, err := helper.CIDString2HashString(cid)
		if err != nil {
			return nil, err
		}
		blocksHashList = append(blocksHashList, blockHash)
	}

	return blocksHashList, nil
}

func (cf *carfile) getCarfileHashString() (string, error) {
	carfileHash, err := helper.CIDString2HashString(cf.carfileCID)
	if err != nil {
		return "", err
	}
	return carfileHash, nil
}

func (cf *carfile) ecodeCarfile() ([]byte, error) {
	return ecodeCarfile(cf)
}

func (cf *carfile) decodeCarfileFromBuffer(carfileData []byte) error {
	return decodeCarfileFromData(carfileData, cf)
}
