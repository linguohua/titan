package carfile

import (
	"context"
	"sync"
	"time"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
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

	go carfileOperation.startCarfileDownloader()
	go carfileOperation.startTickForDownloadResult()

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

func (carfileOperation *CarfileOperation) startTickForDownloadResult() {
	for {
		time.Sleep(time.Minute)

		carfile := carfileOperation.getFirstCarfileFromWaitList()
		if carfile != nil {
			carfileOperation.downloadResult(carfile)
		}
	}
}

func (carfileOperation *CarfileOperation) notifyCarfileDownloader() {
	select {
	case carfileOperation.carfileCacheCh <- true:
	default:
	}
}

func (carfileOperation *CarfileOperation) addCarfile2WaitList(carfile *carfile) {
	carfileOperation.waitListLock.Lock()
	defer carfileOperation.waitListLock.Unlock()

	for _, carfile := range carfileOperation.carfileWaitList {
		if carfile.carfileCID == carfile.carfileCID {
			return
		}
	}

	carfileOperation.carfileWaitList = append(carfileOperation.carfileWaitList, carfile)
}

func (carfileOperation *CarfileOperation) getFirstCarfileFromWaitList() *carfile {
	carfileOperation.waitListLock.Lock()
	defer carfileOperation.waitListLock.Unlock()

	if len(carfileOperation.carfileWaitList) == 0 {
		return nil
	}

	return carfileOperation.carfileWaitList[0]
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

		err = carfile.saveCarfileTable(carfileOperation)
		if err != nil {
			log.Errorf("doDownloadCarfile, saveCarfileTable error:%s", err)
		}

		err = carfileOperation.downloadResult(carfile)
		if err != nil {
			log.Errorf("doDownloadCarfile, saveCarfileTable error:%s", err)
		}

		carfileOperation.removeFirstCarfileFromWaitList()
	}
}

func (carfileOperation *CarfileOperation) downloadResult(carfile *carfile) error {
	ctx, cancel := context.WithTimeout(context.Background(), helper.SchedulerApiTimeout*time.Second)
	defer cancel()

	status := api.CacheStatusFail
	if carfile.downloadSize == carfile.carfileSize {
		status = api.CacheStatusSuccess
	}

	carfileHash, err := helper.CIDString2HashString(carfile.carfileCID)
	if err != nil {
		return err
	}

	deviceInfo, err := carfileOperation.device.DeviceInfo(context.Background())
	if err != nil {
		return err
	}

	result := api.CacheResultInfo{
		Status:      status,
		TotalBlock:  len(carfile.blocksDownloadSuccessList) + len(carfile.blocksWaitList),
		DoneBlocks:  len(carfile.blocksDownloadSuccessList),
		TotalSize:   int(carfile.carfileSize),
		DoneSize:    int(carfile.downloadSize),
		CarfileHash: carfileHash,
		DiskUsage:   deviceInfo.DiskUsage,
	}
	return carfileOperation.scheduler.CacheResult(ctx, result)
}

func (carfileOperation *CarfileOperation) CacheCarfile(ctx context.Context, carfileCID string, sources []*api.DowloadSource) (api.CacheCarfileResult, error) {
	carfile := &carfile{
		carfileCID:                carfileCID,
		blocksWaitList:            make([]string, 0),
		blocksDownloadSuccessList: make([]string, 0),
		blocksDownloadFailedList:  make([]string, 0),
		downloadSources:           sources,
	}

	carfileOperation.addCarfile2WaitList(carfile)
	carfileOperation.notifyCarfileDownloader()

	deviceInfo, err := carfileOperation.device.DeviceInfo(context.Background())
	if err != nil {
		return api.CacheCarfileResult{}, err
	}

	carfileCount, err := carfileOperation.carfileStore.CarfilesCount()
	if err != nil {
		return api.CacheCarfileResult{}, err
	}

	return api.CacheCarfileResult{CacheCarfileCount: carfileCount, WaitCacheCarfileNum: len(carfileOperation.carfileWaitList), DiskUsage: deviceInfo.DiskUsage}, nil
}

func (carfileOperation *CarfileOperation) DeleteCarfile(ctx context.Context, carfileCID string) (int, error) {
	return 0, nil
}
func (carfileOperation *CarfileOperation) DeleteAllCarfiles(ctx context.Context) error {
	return nil
}
func (carfileOperation *CarfileOperation) DeleteWaitCacheCarfile(ctx context.Context, carfileCID string) error {
	return nil

}

func (carfileOperation *CarfileOperation) LoadBlock(ctx context.Context, cid string) ([]byte, error) {
	blockHash, err := helper.CIDString2HashString(cid)
	if err != nil {
		return nil, err
	}
	return carfileOperation.carfileStore.GetBlock(blockHash)
}
