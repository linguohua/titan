package carfile

import (
	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/node/helper"
)

// type downloadSource struct {
// 	downloadURL   string
// 	downloadToken string
// }

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

func (cf *carfile) downloadCarfile(carfileOperation *CarfileOperation) {
	for len(cf.blocksWaitList) > 0 {
		doLen := len(cf.blocksWaitList)
		if doLen > helper.Batch {
			doLen = helper.Batch
		}

		blocks := cf.removeBlocksFromWaitList(doLen)
		cf.downloadBlocks(blocks, carfileOperation)
	}
}

func (cf *carfile) downloadBlocks(cids []string, cfOperation *CarfileOperation) {
	cidMap := make(map[string]bool)
	for _, cid := range cids {
		cidMap[cid] = false
	}

	blocks, err := cfOperation.downloader.downloadBlocks(cids, cf.downloadSources)
	if err != nil {
		log.Errorf("loadBlocksAsync loadBlocks err %v", err)
		return
	}

	for _, b := range blocks {
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

		linksSize := uint64(0)
		cids := make([]string, 0, len(links))
		for _, link := range links {
			cids = append(cids, link.Cid.String())
			linksSize += link.Size
		}

		cf.addBlocks2WaitList(cids)

		delete(cidMap, cidStr)
	}

	if len(cidMap) > 0 {
		retryCids := make([]string, 0, len(cidMap))
		for cid := range cidMap {
			retryCids = append(retryCids, cid)
		}
		cf.downloadBlocks(retryCids, cfOperation)
	}
}
