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
	"github.com/linguohua/titan/node/carfile/downloader"
	"github.com/linguohua/titan/node/cidutil"
	"github.com/linguohua/titan/node/device"
)

var log = logging.Logger("carfile")

type CarfileOperation struct {
	scheduler       api.Scheduler
	device          *device.Device
	downloadMgr     *DownloadMgr
	carfileStore    *carfilestore.CarfileStore
	carfileLinkLock *sync.Mutex
	TotalBlockCount int
	toDeleteCarfile sync.Map
}

func NewCarfileOperation(carfileStore *carfilestore.CarfileStore, scheduler api.Scheduler, blockDownloader downloader.BlockDownloader, device *device.Device) *CarfileOperation {
	carfileOperation := &CarfileOperation{
		scheduler:    scheduler,
		device:       device,
		carfileStore: carfileStore,
		// ds:              ds,
		carfileLinkLock: &sync.Mutex{},
	}

	carfileOperation.downloadMgr = newDownloadMgr(carfileStore, &downloadOperation{carfileOperation: carfileOperation, downloader: blockDownloader})

	totalBlockCount, err := carfileStore.BlockCount()
	if err != nil {
		log.Panicf("NewCarfileOperation block count error:%s", err.Error())
	}
	carfileOperation.TotalBlockCount = totalBlockCount

	legacy.RegisterCodec(cid.DagProtobuf, dagpb.Type.PBNode, merkledag.ProtoNodeConverter)
	legacy.RegisterCodec(cid.Raw, basicnode.Prototype.Bytes, merkledag.RawNodeConverter)

	return carfileOperation
}

func (carfileOperation *CarfileOperation) downloadResult(carfile *carfileCache, isComplete bool) error {
	status := api.CacheStatusFailed
	if !isComplete {
		status = api.CacheStatusDownloading
	} else {
		// count total block in filesystem is cost much time
		// only do it on carfile download complete
		totalBlockCount, err := carfileOperation.carfileStore.BlockCount()
		if err == nil {
			carfileOperation.TotalBlockCount = totalBlockCount
		} else {
			log.Errorf("downloadResult block count error:%s", err.Error())
		}
	}

	if carfile.carfileSize != 0 && carfile.downloadSize == carfile.carfileSize {
		status = api.CacheStatusSucceeded
	}

	carfileHash, err := cidutil.CIDString2HashString(carfile.carfileCID)
	if err != nil {
		return err
	}

	_, diskUsage := carfileOperation.device.GetDiskUsageStat()

	result := api.CacheResultInfo{
		Status:            status,
		CarfileBlockCount: len(carfile.blocksDownloadSuccessList) + len(carfile.blocksWaitList),
		DoneBlockCount:    len(carfile.blocksDownloadSuccessList),
		CarfileSize:       int64(carfile.carfileSize),
		DoneSize:          int64(carfile.downloadSize),
		CarfileHash:       carfileHash,
		DiskUsage:         diskUsage,
		TotalBlockCount:   carfileOperation.TotalBlockCount,
	}

	ctx, cancel := context.WithTimeout(context.Background(), schedulerApiTimeout*time.Second)
	defer cancel()

	log.Infof("downloadResult, carfile:%s", result.CarfileHash)
	return carfileOperation.scheduler.CacheResult(ctx, result)
}

func (carfileOperation *CarfileOperation) cacheCarfileResult() (*api.CacheCarfileResult, error) {
	_, diskUsage := carfileOperation.device.GetDiskUsageStat()

	carfileCount, err := carfileOperation.carfileStore.CarfileCount()
	if err != nil {
		return nil, err
	}

	return &api.CacheCarfileResult{CacheCarfileCount: carfileCount, WaitCacheCarfileNum: carfileOperation.downloadMgr.waitListLen(), DiskUsage: diskUsage}, nil
}

func (carfileOperation *CarfileOperation) cacheResultForCarfileExist(carfileCID string) error {
	_, diskUsage := carfileOperation.device.GetDiskUsageStat()

	carfileHash, err := cidutil.CIDString2HashString(carfileCID)
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
	for _, link := range links {
		linksSize += link.Size
	}

	result := api.CacheResultInfo{
		Status:            api.CacheStatusSucceeded,
		CarfileBlockCount: blocksCount,
		DoneBlockCount:    blocksCount,
		CarfileSize:       int64(linksSize),
		DoneSize:          int64(linksSize),
		CarfileHash:       carfileHash,
		DiskUsage:         diskUsage,
		TotalBlockCount:   carfileOperation.TotalBlockCount,
	}

	ctx, cancel := context.WithTimeout(context.Background(), schedulerApiTimeout*time.Second)
	defer cancel()

	return carfileOperation.scheduler.CacheResult(ctx, result)
}

func (carfileOperation *CarfileOperation) deleteWaitCacheCarfile(carfileCID string) (int, error) {
	carfile := carfileOperation.downloadMgr.removeCarfileFromWaitList(carfileCID)
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

	carfileOperation.downloadMgr.saveWaitList()
	return len(hashs), nil
}

func (carfileOperation *CarfileOperation) deleteCarfile(carfileCID string) (int, error) {
	if carfileOperation.downloadMgr.isCarfileInWaitList(carfileCID) {
		return carfileOperation.deleteWaitCacheCarfile(carfileCID)
	}

	carfileHash, err := cidutil.CIDString2HashString(carfileCID)
	if err != nil {
		return 0, err
	}

	hashs, err := carfileOperation.carfileStore.GetBlocksHashOfCarfile(carfileHash)
	if err == datastore.ErrNotFound {
		data, err := carfileOperation.carfileStore.GetIncompleteCarfileCacheData(carfileHash)
		if err != nil {
			return 0, err
		}

		carfile := &carfileCache{}
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
		err = carfileOperation.deleteBlock(hash, carfileHash)
		if err != nil {
			if err == datastore.ErrNotFound {
				log.Warnf("deleteCarfile, multiple block %s in carfile %s", hash, carfileHash)
			} else {
				log.Errorf("deleteCarfile deleteBlock %s error:%s", hash, err.Error())
			}
		}
	}

	carfileOperation.carfileStore.DeleteCarfileTable(carfileHash)
	carfileOperation.carfileStore.DeleteIncompleteCarfileCache(carfileHash)

	return len(hashs), nil
}

func (carfileOperation *CarfileOperation) GetBlocksOfCarfile(carfileCID string, indexs []int) (map[int]string, error) {
	carfileHash, err := cidutil.CIDString2HashString(carfileCID)
	if err != nil {
		log.Errorf("GetBlocksOfCarfile, CIDString2HashString error:%s, carfileCID:%s", err.Error(), carfileCID)
		return nil, err
	}

	blocksHash, err := carfileOperation.carfileStore.GetBlocksHashWithCarfilePositions(carfileHash, indexs)
	if err != nil {
		return nil, err
	}

	ret := make(map[int]string)
	for index, blockHash := range blocksHash {
		cid, err := cidutil.HashString2CIDString(blockHash)
		if err != nil {
			log.Errorf("GetBlocksOfCarfile, can not convert hash %s to cid", blockHash)
			continue
		}

		pos := indexs[index]
		ret[pos] = cid
	}

	return ret, nil
}

func (carfileOperation *CarfileOperation) BlockCountOfCarfile(carfileCID string) (int, error) {
	carfileHash, err := cidutil.CIDString2HashString(carfileCID)
	if err != nil {
		return 0, err
	}
	return carfileOperation.carfileStore.BlockCountOfCarfile(carfileHash)
}
