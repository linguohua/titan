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
	blocksDownloadFailedList  []string
	downloadSources           []*api.DowloadSource
	carfileSize               int64
	downloadSize              int64
}

func newCarfileCache(carfileCID string, downloadSources []*api.DowloadSource) *carfile {
	return &carfile{
		carfileCID:                carfileCID,
		blocksDownloadSuccessList: make([]string, 0),
		blocksDownloadFailedList:  make([]string, 0),
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
	ret, err := cf.downloadBlocksWithBreadthFirst([]string{cf.carfileCID}, carfileOperation)
	if err != nil {
		return err
	}
	cf.carfileSize = int64(ret.linksSize) + int64(ret.downloadSize)

	for len(ret.netLayerCids) > 0 {
		ret, err = cf.downloadBlocksWithBreadthFirst(ret.netLayerCids, carfileOperation)
		if err != nil {
			return err
		}

	}
	return nil
}

func (cf *carfile) downloadBlocksWithBreadthFirst(layerCids []string, cfOperation *CarfileOperation) (result *downloadResult, err error) {
	cf.blocksWaitList = layerCids
	result = &downloadResult{}
	for len(cf.blocksWaitList) > 0 {
		doLen := len(cf.blocksWaitList)
		if doLen > helper.Batch {
			doLen = helper.Batch
		}

		blocks := cf.getBlocksFromWaitListFront(doLen)
		ret, err := cf.downloadBlocks(blocks, cfOperation)
		if err != nil {
			cf.blocksDownloadFailedList = append(cf.blocksDownloadFailedList, blocks...)
			return nil, err
		}

		cf.blocksDownloadSuccessList = append(cf.blocksDownloadSuccessList, blocks...)
		cf.downloadSize += int64(ret.downloadSize)

		result.linksSize += ret.linksSize
		result.downloadSize += ret.downloadSize
		result.netLayerCids = append(result.netLayerCids, ret.netLayerCids...)

		cf.removeBlocksFromWaitList(doLen)
	}

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

func (cf *carfile) saveCarfileTable(cfOperation *CarfileOperation) error {
	log.Infof("saveCarfileTable, carfile cid:%s, download size:%d,carfileSize:%d", cf.carfileCID, cf.downloadSize, cf.carfileSize)
	carfileHash, err := cf.getCarfileHashString()
	if err != nil {
		return err
	}

	if cf.downloadSize != 0 && cf.downloadSize == cf.carfileSize {
		blocksHashString, err := cf.blockList2BlocksHashString()
		if err != nil {
			return err
		}

		cfOperation.carfileStore.DeleteIncompleteCarfile(carfileHash)
		return cfOperation.carfileStore.SaveBlockListOfCarfile(carfileHash, blocksHashString)
	} else {
		buf, err := cf.ecodeCarfile()
		if err != nil {
			return err
		}

		cfOperation.carfileStore.DeleteCarfileTable(carfileHash)
		return cfOperation.carfileStore.SaveIncomleteCarfile(carfileHash, buf)
	}
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
	return decodeCarfileFromBuffer(carfileData, cf)
}
