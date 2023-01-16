package carfile

import (
	"context"
	"sync"
	"time"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	legacy "github.com/ipfs/go-ipld-legacy"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipfs/go-merkledag"
	dagpb "github.com/ipld/go-codec-dagpb"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/node/carfile/carfilestore"
	"github.com/linguohua/titan/node/device"
	"github.com/linguohua/titan/node/helper"
)

var log = logging.Logger("carfile")

type CarfileOperation struct {
	scheduler api.Scheduler
	// carfile list
	carfileWaitList []*carfile
	waitListLock    *sync.Mutex
	downloader      BlockDownloader
	device          *device.Device
	carfileCacheCh  chan bool
	carfileStore    *carfilestore.CarfileStore
}

type BlockDownloader interface {
	// scheduler request cache carfile
	downloadBlocks(cids []string, sources []*api.DowloadSource) ([]blocks.Block, error)
	// // local sync miss data
	// syncData(block *Block, reqs map[int]string) error
}

func NewCarfileOperation(carfileStore *carfilestore.CarfileStore, scheduler api.Scheduler, blockDownloader BlockDownloader, device *device.Device) *CarfileOperation {
	carfileOperation := &CarfileOperation{
		scheduler:       scheduler,
		downloader:      blockDownloader,
		carfileWaitList: make([]*carfile, 0),
		waitListLock:    &sync.Mutex{},
		device:          device,
		carfileCacheCh:  make(chan bool),
		carfileStore:    carfileStore,
	}

	carfileOperation.restoreWaitListFromFile()

	go carfileOperation.startCarfileDownloader()
	go carfileOperation.startTick()
	go carfileOperation.delayDownloadTask()

	legacy.RegisterCodec(cid.DagProtobuf, dagpb.Type.PBNode, merkledag.ProtoNodeConverter)
	legacy.RegisterCodec(cid.Raw, basicnode.Prototype.Bytes, merkledag.RawNodeConverter)

	return carfileOperation
}

func (carfileOperation *CarfileOperation) startCarfileDownloader() {
	if carfileOperation.downloader == nil {
		log.Panic("block.block == nil")
	}

	for {
		<-carfileOperation.carfileCacheCh
		carfileOperation.doDownloadCarfile()
	}
}

func (carfileOperation *CarfileOperation) delayDownloadTask() {
	time.Sleep(3 * time.Second)
	carfileOperation.notifyCarfileDownloader()
}

func (carfileOperation *CarfileOperation) startTick() {
	for {
		time.Sleep(time.Minute)

		carfile := carfileOperation.getFirstCarfileFromWaitList()
		if carfile != nil {
			err := carfileOperation.downloadResult(carfile, false)
			if err != nil {
				log.Errorf("startTickForDownloadResult, downloadResult error:%s", err.Error())
			}
		}

		if len(carfileOperation.carfileWaitList) > 0 {
			carfileOperation.saveWaitList()
		}
	}
}

func (carfileOperation *CarfileOperation) notifyCarfileDownloader() {
	select {
	case carfileOperation.carfileCacheCh <- true:
	default:
	}
}

func (carfileOperation *CarfileOperation) addCarfile2WaitList(cf *carfile) {
	carfileOperation.waitListLock.Lock()
	defer carfileOperation.waitListLock.Unlock()

	for _, carfile := range carfileOperation.carfileWaitList {
		if carfile.carfileCID == cf.carfileCID {
			return
		}
	}

	carfileOperation.carfileWaitList = append(carfileOperation.carfileWaitList, cf)
}

func (carfileOperation *CarfileOperation) isCarfileInWaitList(carfileCID string) bool {
	carfileOperation.waitListLock.Lock()
	defer carfileOperation.waitListLock.Unlock()

	for _, carfile := range carfileOperation.carfileWaitList {
		if carfile.carfileCID == carfileCID {
			return true
		}
	}

	return false
}

func (carfileOperation *CarfileOperation) getFirstCarfileFromWaitList() *carfile {
	carfileOperation.waitListLock.Lock()
	defer carfileOperation.waitListLock.Unlock()

	if len(carfileOperation.carfileWaitList) == 0 {
		return nil
	}

	return carfileOperation.carfileWaitList[0]
}

func (carfileOperation *CarfileOperation) removeCarfileFromWaitList(carfileCID string) *carfile {
	carfileOperation.waitListLock.Lock()
	defer carfileOperation.waitListLock.Unlock()

	for i, carfile := range carfileOperation.carfileWaitList {
		if carfile.carfileCID == carfileCID {
			carfileOperation.carfileWaitList = append(carfileOperation.carfileWaitList[0:i], carfileOperation.carfileWaitList[i+1:]...)
			return carfile
		}
	}

	return nil
}

func (carfileOperation *CarfileOperation) removeFirstCarfileFromWaitList() *carfile {
	carfileOperation.waitListLock.Lock()
	defer carfileOperation.waitListLock.Unlock()

	if len(carfileOperation.carfileWaitList) == 0 {
		return nil
	}

	carfile := carfileOperation.carfileWaitList[0]
	carfileOperation.carfileWaitList = carfileOperation.carfileWaitList[1:]

	return carfile
}

func (carfileOperation *CarfileOperation) doDownloadCarfile() {
	for len(carfileOperation.carfileWaitList) > 0 {
		carfile := carfileOperation.getFirstCarfileFromWaitList()
		err := carfile.downloadCarfile(carfileOperation)
		if err != nil {
			log.Errorf("doDownloadCarfile, downloadCarfile error:%s", err)
		}
		carfileOperation.removeFirstCarfileFromWaitList()

		carfileOperation.onDownloadCarfileComplete(carfile)
	}
}

func (carfileOperation *CarfileOperation) saveCarfileTable(cf *carfile) error {
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

		carfileOperation.carfileStore.DeleteIncompleteCarfile(carfileHash)
		return carfileOperation.carfileStore.SaveBlockListOfCarfile(carfileHash, blocksHashString)
	} else {
		buf, err := cf.ecodeCarfile()
		if err != nil {
			return err
		}

		carfileOperation.carfileStore.DeleteCarfileTable(carfileHash)
		return carfileOperation.carfileStore.SaveIncomleteCarfile(carfileHash, buf)
	}
}

func (carfileOperation *CarfileOperation) saveWaitList() error {
	data, err := ecodeWaitList(carfileOperation.carfileWaitList)
	if err != nil {
		return err
	}

	return carfileOperation.carfileStore.SaveWaitListToFile(data)
}

func (carfileOperation *CarfileOperation) downloadResult(carfile *carfile, isComplete bool) error {
	ctx, cancel := context.WithTimeout(context.Background(), helper.SchedulerApiTimeout*time.Second)
	defer cancel()

	status := api.CacheStatusFail
	if !isComplete {
		status = api.CacheStatusCreate
	}

	if carfile.carfileSize != 0 && carfile.downloadSize == carfile.carfileSize {
		status = api.CacheStatusSuccess
	}

	carfileHash, err := helper.CIDString2HashString(carfile.carfileCID)
	if err != nil {
		return err
	}

	_, diskUsage := carfileOperation.device.GetDiskUsageStat()

	result := api.CacheResultInfo{
		Status:      status,
		TotalBlock:  len(carfile.blocksDownloadSuccessList) + len(carfile.blocksWaitList),
		DoneBlocks:  len(carfile.blocksDownloadSuccessList),
		TotalSize:   int64(carfile.carfileSize),
		DoneSize:    int64(carfile.downloadSize),
		CarfileHash: carfileHash,
		DiskUsage:   diskUsage,
	}
	return carfileOperation.scheduler.CacheResult(ctx, result)
}

func (carfileOperation *CarfileOperation) onDownloadCarfileComplete(cf *carfile) {
	log.Infof("onDownloadCarfileComplete, carfile %s", cf.carfileCID)
	err := carfileOperation.saveCarfileTable(cf)
	if err != nil {
		log.Errorf("onDownloadCarfileComplete, saveCarfileTable error:%s", err)
	}

	err = carfileOperation.saveWaitList()
	if err != nil {
		log.Errorf("onDownloadCarfileComplete, saveCarfileTable error:%s", err)
	}

	err = carfileOperation.downloadResult(cf, true)
	if err != nil {
		log.Errorf("onDownloadCarfileComplete, downloadResult error:%s", err)
	}
}

func (carfileOperation *CarfileOperation) cacheCarfileResult() (api.CacheCarfileResult, error) {
	_, diskUsage := carfileOperation.device.GetDiskUsageStat()

	carfileCount, err := carfileOperation.carfileStore.CarfileCount()
	if err != nil {
		return api.CacheCarfileResult{}, err
	}

	return api.CacheCarfileResult{CacheCarfileCount: carfileCount, WaitCacheCarfileNum: len(carfileOperation.carfileWaitList), DiskUsage: diskUsage}, nil
}

func (carfileOperation *CarfileOperation) cacheResultForCarfileExist(carfileCID string) error {
	_, diskUsage := carfileOperation.device.GetDiskUsageStat()

	carfileHash, err := helper.CIDString2HashString(carfileCID)
	if err != nil {
		return err
	}

	blocksCount, err := carfileOperation.carfileStore.BlockCountOfCarfile(carfileHash)
	if err != nil {
		return err
	}

	data, err := carfileOperation.carfileStore.GetBlock(carfileHash)
	if err != nil {
		return err
	}

	cid, err := cid.Decode(carfileCID)
	if err != nil {
		return err
	}

	b, err := blocks.NewBlockWithCid(data, cid)
	if err != nil {
		return err
	}

	links, err := resolveLinks(b)
	if err != nil {
		return err
	}

	linksSize := uint64(len(data))
	cids := make([]string, 0, len(links))
	for _, link := range links {
		cids = append(cids, link.Cid.String())
		linksSize += link.Size
	}

	result := api.CacheResultInfo{
		Status:      api.CacheStatusSuccess,
		TotalBlock:  blocksCount,
		DoneBlocks:  blocksCount,
		TotalSize:   int64(linksSize),
		DoneSize:    int64(linksSize),
		CarfileHash: carfileHash,
		DiskUsage:   diskUsage,
	}

	ctx, cancel := context.WithTimeout(context.Background(), helper.SchedulerApiTimeout*time.Second)
	defer cancel()

	return carfileOperation.scheduler.CacheResult(ctx, result)
}

func (carfileOperation *CarfileOperation) restoreWaitListFromFile() {
	data, err := carfileOperation.carfileStore.GetWaitListFromFile()
	if err != nil {
		if err != datastore.ErrNotFound {
			log.Errorf("getWaitListFromFile error:%s", err)
		}
		return
	}

	carfileOperation.carfileWaitList, err = decodeWaitListFromData(data)
	if err != nil {
		log.Errorf("getWaitListFromFile error:%s", err)
		return
	}

	log.Infof("restoreWaitListFromFile:%v", carfileOperation.carfileWaitList)
}

func (carfileOperation *CarfileOperation) deleteCarfile(carfileCID string) (int, error) {
	if carfileOperation.isCarfileInWaitList(carfileCID) {
		return carfileOperation.DeleteWaitCacheCarfile(context.Background(), carfileCID)
	}

	carfileHash, err := helper.CIDString2HashString(carfileCID)
	if err != nil {
		return 0, err
	}

	hashs, err := carfileOperation.carfileStore.GetBlocksHashOfCarfile(carfileHash)
	if err == datastore.ErrNotFound {
		data, err := carfileOperation.carfileStore.GetIncomleteCarfileData(carfileHash)
		if err != nil {
			return 0, err
		}

		carfile := &carfile{}
		err = carfile.decodeCarfileFromBuffer(data)
		if err != nil {
			return 0, err
		}

		hashs, err = carfile.blockCidList2BlocksHashList()
		if err != nil {
			return 0, err
		}

	}

	for _, hash := range hashs {
		err = carfileOperation.carfileStore.DeleteBlock(hash)
		if err != nil {
			log.Errorf("delete block %s error:%s", hash, err.Error())
		}
	}

	carfileOperation.carfileStore.DeleteCarfileTable(carfileHash)
	carfileOperation.carfileStore.DeleteIncompleteCarfile(carfileHash)

	return len(hashs), nil
}

func (carfileOperation *CarfileOperation) CacheCarfile(ctx context.Context, carfileCID string, sources []*api.DowloadSource) (api.CacheCarfileResult, error) {
	cf := &carfile{
		carfileCID:                carfileCID,
		blocksWaitList:            make([]string, 0),
		blocksDownloadSuccessList: make([]string, 0),
		nextLayerCIDs:             make([]string, 0),
		downloadSources:           sources,
	}

	carfileHash, err := helper.CIDString2HashString(carfileCID)
	if err != nil {
		return api.CacheCarfileResult{}, err
	}

	has, err := carfileOperation.carfileStore.HasCarfile(carfileHash)
	if err != nil {
		return api.CacheCarfileResult{}, err
	}

	if has {
		err = carfileOperation.cacheResultForCarfileExist(carfileCID)
		if err != nil {
			log.Errorf("CacheCarfile, cacheResultForCarfileExist error:%s", err.Error())
		}

		log.Infof("carfile %s carfileCID aready exist, not need to cache", carfileCID)

		return carfileOperation.cacheCarfileResult()
	}

	data, err := carfileOperation.carfileStore.GetIncomleteCarfileData(carfileHash)
	if err == nil {
		err = decodeCarfileFromData(data, cf)
	}

	if err != nil && err != datastore.ErrNotFound {
		log.Errorf("CacheCarfile load incomplete carfile error %s", err.Error())
	}

	carfileOperation.addCarfile2WaitList(cf)
	carfileOperation.saveWaitList()
	carfileOperation.notifyCarfileDownloader()

	return carfileOperation.cacheCarfileResult()
}

func (carfileOperation *CarfileOperation) DeleteCarfile(ctx context.Context, carfileCID string) error {
	go func() {
		_, err := carfileOperation.deleteCarfile(carfileCID)
		if err != nil {
			log.Errorf("DeleteCarfile, delete carfile error:%s", err.Error())
		}

		ctx, cancel := context.WithTimeout(context.Background(), helper.SchedulerApiTimeout*time.Second)
		defer cancel()

		blockCount, err := carfileOperation.carfileStore.BlockCount()
		if err != nil {
			log.Errorf("DeleteCarfile, BlockCount error:%s", err.Error())
		}

		_, diskUsage := carfileOperation.device.GetDiskUsageStat()
		info := api.RemoveCarfileResultInfo{BlockCount: blockCount, DiskUsage: diskUsage}

		err = carfileOperation.scheduler.RemoveCarfileResult(ctx, info)
		if err != nil {
			log.Errorf("DeleteCarfile, RemoveCarfileResult error:%s", err.Error())
		}

	}()
	return nil
}
func (carfileOperation *CarfileOperation) DeleteAllCarfiles(ctx context.Context) error {
	return nil
}
func (carfileOperation *CarfileOperation) DeleteWaitCacheCarfile(ctx context.Context, carfileCID string) (int, error) {
	carfile := carfileOperation.removeCarfileFromWaitList(carfileCID)
	if carfile == nil {
		return 0, nil
	}

	if len(carfile.blocksDownloadSuccessList) == 0 {
		return 0, nil
	}

	hashs, err := carfile.blockCidList2BlocksHashList()
	if err != nil {
		return 0, err
	}

	for _, hash := range hashs {
		err = carfileOperation.carfileStore.DeleteBlock(hash)
		if err != nil {
			log.Errorf("delete block error:%s", err.Error())
		}
	}
	return len(hashs), nil

}

func (carfileOperation *CarfileOperation) LoadBlock(ctx context.Context, cid string) ([]byte, error) {
	blockHash, err := helper.CIDString2HashString(cid)
	if err != nil {
		return nil, err
	}
	return carfileOperation.carfileStore.GetBlock(blockHash)
}
