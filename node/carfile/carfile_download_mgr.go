package carfile

import (
	"sync"
	"time"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-datastore"
	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/node/carfile/carfilestore"
)

type DownloadOperation interface {
	downloadResult(carfile *carfileCache, isComplete bool) error
	downloadBlocks(cids []string, sources []*api.DowloadSource) ([]blocks.Block, error)
	saveBlock(data []byte, blockHash, carfileHash string) error
}
type DownloadMgr struct {
	waitList          []*carfileCache
	waitListLock      *sync.Mutex
	carfileCacheCh    chan bool
	carfileStore      *carfilestore.CarfileStore
	downloadOperation DownloadOperation
}

func newDownloadMgr(carfileStore *carfilestore.CarfileStore, downloadOperation DownloadOperation) *DownloadMgr {
	downloadMgr := &DownloadMgr{
		waitList:          make([]*carfileCache, 0),
		waitListLock:      &sync.Mutex{},
		carfileCacheCh:    make(chan bool),
		carfileStore:      carfileStore,
		downloadOperation: downloadOperation,
	}
	downloadMgr.restoreWaitListFromFile()

	go downloadMgr.startCarfileDownloader()
	go downloadMgr.startTick()
	go downloadMgr.delayNotifyDownloaderOnce()

	return downloadMgr
}
func (downloadMgr *DownloadMgr) delayNotifyDownloaderOnce() {
	time.AfterFunc(3*time.Second, downloadMgr.notifyCarfileDownloader)
}

func (downloadMgr *DownloadMgr) startTick() {
	ticker := time.NewTicker(10 * time.Second)
	for {
		select {
		case <-ticker.C:
			carfile := downloadMgr.getFirstCarfileCacheFromWaitList()
			if carfile != nil {
				err := downloadMgr.downloadOperation.downloadResult(carfile, false)
				if err != nil {
					log.Errorf("startTickForDownloadResult, downloadResult error:%s", err.Error())
				}
			}

			if len(downloadMgr.waitList) > 0 {
				downloadMgr.saveWaitList()
			}
		}
	}
}

func (downloadMgr *DownloadMgr) notifyCarfileDownloader() {
	select {
	case downloadMgr.carfileCacheCh <- true:
	default:
	}
}

func (downloadMgr *DownloadMgr) startCarfileDownloader() {
	if downloadMgr.downloadOperation == nil {
		log.Panic("downloadMgr.downloadOperation == nil")
	}

	for {
		<-downloadMgr.carfileCacheCh
		downloadMgr.doDownloadCarfile()
	}
}

func (downloadMgr *DownloadMgr) doDownloadCarfile() {
	for len(downloadMgr.waitList) > 0 {
		carfile := downloadMgr.getFirstCarfileCacheFromWaitList()
		err := carfile.downloadCarfile(downloadMgr.downloadOperation)
		if err != nil {
			log.Errorf("doDownloadCarfile, downloadCarfile error:%s", err)
		}
		downloadMgr.removeFirstCarfileFromWaitList()

		downloadMgr.onDownloadCarfileComplete(carfile)
	}
}

func (downloadMgr *DownloadMgr) removeFirstCarfileFromWaitList() *carfileCache {
	downloadMgr.waitListLock.Lock()
	defer downloadMgr.waitListLock.Unlock()

	if len(downloadMgr.waitList) == 0 {
		return nil
	}

	cfCache := downloadMgr.waitList[0]
	downloadMgr.waitList = downloadMgr.waitList[1:]

	return cfCache
}

func (downloadMgr *DownloadMgr) getFirstCarfileCacheFromWaitList() *carfileCache {
	downloadMgr.waitListLock.Lock()
	defer downloadMgr.waitListLock.Unlock()

	if len(downloadMgr.waitList) == 0 {
		return nil
	}

	return downloadMgr.waitList[0]
}
func (downloadMgr *DownloadMgr) addCarfileCacheToWaitList(cfCache *carfileCache) {
	downloadMgr.waitListLock.Lock()
	defer downloadMgr.waitListLock.Unlock()

	for _, carfile := range downloadMgr.waitList {
		if carfile.carfileCID == cfCache.carfileCID {
			return
		}
	}

	downloadMgr.waitList = append(downloadMgr.waitList, cfCache)

	downloadMgr.saveWaitList()
	downloadMgr.notifyCarfileDownloader()
}

func (downloadMgr *DownloadMgr) isCarfileInWaitList(carfileCID string) bool {
	downloadMgr.waitListLock.Lock()
	defer downloadMgr.waitListLock.Unlock()

	for _, carfile := range downloadMgr.waitList {
		if carfile.carfileCID == carfileCID {
			return true
		}
	}

	return false
}

func (downloadMgr *DownloadMgr) removeCarfileFromWaitList(carfileCID string) *carfileCache {
	downloadMgr.waitListLock.Lock()
	defer downloadMgr.waitListLock.Unlock()

	for i, carfile := range downloadMgr.waitList {
		if carfile.carfileCID == carfileCID {
			downloadMgr.waitList = append(downloadMgr.waitList[0:i], downloadMgr.waitList[i+1:]...)
			return carfile
		}
	}

	return nil
}

func (downloadMgr *DownloadMgr) onDownloadCarfileComplete(cf *carfileCache) {
	log.Infof("onDownloadCarfileComplete, carfile %s", cf.carfileCID)
	err := downloadMgr.saveCarfileTable(cf)
	if err != nil {
		log.Errorf("onDownloadCarfileComplete, saveCarfileTable error:%s", err)
	}

	err = downloadMgr.saveWaitList()
	if err != nil {
		log.Errorf("onDownloadCarfileComplete, saveCarfileTable error:%s", err)
	}

	err = downloadMgr.downloadOperation.downloadResult(cf, true)
	if err != nil {
		log.Errorf("onDownloadCarfileComplete, downloadResult error:%s, carfileCID:%s", err, cf.carfileCID)
	}
}

func (downloadMgr *DownloadMgr) saveCarfileTable(cf *carfileCache) error {
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

		downloadMgr.carfileStore.DeleteIncompleteCarfile(carfileHash)
		return downloadMgr.carfileStore.SaveBlockListOfCarfile(carfileHash, blocksHashString)
	} else {
		buf, err := cf.ecodeCarfile()
		if err != nil {
			return err
		}

		downloadMgr.carfileStore.DeleteCarfileTable(carfileHash)
		return downloadMgr.carfileStore.SaveIncomleteCarfile(carfileHash, buf)
	}
}

func (downloadMgr *DownloadMgr) saveWaitList() error {
	data, err := ecodeWaitList(downloadMgr.waitList)
	if err != nil {
		return err
	}

	return downloadMgr.carfileStore.SaveWaitListToFile(data)
}

func (downloadMgr *DownloadMgr) restoreWaitListFromFile() {
	data, err := downloadMgr.carfileStore.GetWaitListFromFile()
	if err != nil {
		if err != datastore.ErrNotFound {
			log.Errorf("getWaitListFromFile error:%s", err)
		}
		return
	}

	downloadMgr.waitList, err = decodeWaitListFromData(data)
	if err != nil {
		log.Errorf("getWaitListFromFile error:%s", err)
		return
	}

	log.Infof("restoreWaitListFromFile:%v", downloadMgr.waitList)
}

func (downloadMgr *DownloadMgr) waitListLen() int {
	return len(downloadMgr.waitList)
}
