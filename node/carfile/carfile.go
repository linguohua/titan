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
	// waitListLock              *sync.Mutex
}

func newCarfileCache(carfileCID string, downloadSources []*api.DowloadSource) *carfile {
	return &carfile{
		carfileCID:                carfileCID,
		blocksWaitList:            []string{carfileCID},
		blocksDownloadSuccessList: make([]string, 0),
		blocksDownloadFailedList:  make([]string, 0),
		downloadSources:           downloadSources}
	// waitListLock:              &sync.Mutex{}
}

func (cf *carfile) removeBlocksFromWaitList(n int) []string {
	// cf.waitListLock.Lock()
	// defer cf.waitListLock.Unlock()

	if len(cf.blocksWaitList) < n {
		n = len(cf.blocksWaitList)
	}

	blocks := cf.blocksWaitList[:n]
	cf.blocksWaitList = cf.blocksWaitList[n:]

	return blocks
}

func (cf *carfile) addBlocks2WaitList(blocks []string) {
	// cf.waitListLock.Lock()
	// defer cf.waitListLock.Unlock()

	cf.blocksWaitList = append(cf.blocksWaitList, blocks...)
}

func (cf *carfile) downloadCarfile(carfileOperation *CarfileOperation) error {
	ret, err := cf.downloadBlocksWithBreadthFirst([]string{cf.carfileCID}, carfileOperation)
	if err != nil {
		return err
	}

	cf.carfileSize = int64(ret.linksSize)

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

		blocks := cf.removeBlocksFromWaitList(doLen)
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
	}

	return result, nil
}

func (cf *carfile) downloadBlocks(cids []string, cfOperation *CarfileOperation) (*downloadResult, error) {
	blks, err := cfOperation.downloader.downloadBlocks(cids, cf.downloadSources)
	if err != nil {
		log.Errorf("loadBlocksAsync loadBlocks err %v", err)
		return nil, err
	}

	if len(blks) != len(cids) {
		return nil, fmt.Errorf("download blocks failed")
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

		downloadSize = uint64(len(b.RawData()))
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

// func (cf *carfile) downloadBlocks(cids []string, cfOperation *CarfileOperation) {
// 	cidMap := make(map[string]bool)
// 	for _, cid := range cids {
// 		cidMap[cid] = false
// 	}

// 	blocks, err := cfOperation.downloader.downloadBlocks(cids, cf.downloadSources)
// 	if err != nil {
// 		log.Errorf("loadBlocksAsync loadBlocks err %v", err)
// 		return
// 	}

// 	for _, b := range blocks {
// 		cidStr := b.Cid().String()
// 		err = cfOperation.saveBlock(b.RawData(), cidStr)
// 		if err != nil {
// 			log.Errorf("loadBlocksFromIPFS save block error:%s", err.Error())
// 			continue
// 		}

// 		// get block links
// 		links, err := resolveLinks(b)
// 		if err != nil {
// 			log.Errorf("loadBlocksFromIPFS resolveLinks error:%s", err.Error())
// 			continue
// 		}

// 		linksSize := uint64(0)
// 		cids := make([]string, 0, len(links))
// 		for _, link := range links {
// 			cids = append(cids, link.Cid.String())
// 			linksSize += link.Size
// 		}

// 		cf.addBlocks2WaitList(cids)

// 		delete(cidMap, cidStr)
// 	}

// 	if len(cidMap) > 0 {
// 		retryCids := make([]string, 0, len(cidMap))
// 		for cid := range cidMap {
// 			retryCids = append(retryCids, cid)
// 		}
// 		cf.downloadBlocks(retryCids, cfOperation)
// 	}
// }
