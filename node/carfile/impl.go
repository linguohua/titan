package carfile

import (
	"context"
	"fmt"
	"time"

	"github.com/ipfs/go-datastore"
	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/node/helper"
)

func (carfileOperation *CarfileOperation) CacheCarfile(ctx context.Context, carfileCID string, sources []*api.DowloadSource) (*api.CacheCarfileResult, error) {
	carfileHash, err := helper.CIDString2HashString(carfileCID)
	if err != nil {
		log.Errorf("CacheCarfile, CIDString2HashString error:%s, carfile cid:%s", err.Error(), carfileCID)
		return nil, err
	}

	_, ok := carfileOperation.toDeleteCarfile.Load(carfileHash)
	if ok {
		return nil, fmt.Errorf("Carfile %s is to delete, can not cache", carfileCID)
	}

	has, err := carfileOperation.carfileStore.HasCarfile(carfileHash)
	if err != nil {
		log.Errorf("CacheCarfile, HasCarfile error:%s, carfile hash :%s", err.Error(), carfileHash)
		return nil, err
	}

	if has {
		err = carfileOperation.cacheResultForCarfileExist(carfileCID)
		if err != nil {
			log.Errorf("CacheCarfile, cacheResultForCarfileExist error:%s", err.Error())
		}

		log.Infof("carfile %s carfileCID aready exist, not need to cache", carfileCID)

		return carfileOperation.cacheCarfileResult()
	}

	cfCache := &carfileCache{
		carfileCID:                carfileCID,
		blocksWaitList:            make([]string, 0),
		blocksDownloadSuccessList: make([]string, 0),
		nextLayerCIDs:             make([]string, 0),
		downloadSources:           sources,
	}

	data, err := carfileOperation.carfileStore.GetIncomleteCarfileData(carfileHash)
	if err != nil && err != datastore.ErrNotFound {
		log.Errorf("CacheCarfile load incomplete carfile error %s", err.Error())
	}

	if err == nil {
		err = decodeCarfileFromData(data, cfCache)
		if err != nil {
			log.Errorf("CacheCarfile, decodeCarfileFromData error:%s", err.Error())
		} else {
			// reassigned downloadSources to new
			cfCache.downloadSources = sources
		}
	}

	carfileOperation.downloadMgr.addCarfileCacheToWaitList(cfCache)
	log.Infof("CacheCarfile carfile cid:%s", carfileCID)
	return carfileOperation.cacheCarfileResult()
}

func (carfileOperation *CarfileOperation) DeleteCarfile(ctx context.Context, carfileCID string) error {
	carfileHash, err := helper.CIDString2HashString(carfileCID)
	if err != nil {
		return err
	}

	_, ok := carfileOperation.toDeleteCarfile.Load(carfileHash)
	if ok {
		return nil
	}

	go func() {
		carfileOperation.toDeleteCarfile.Store(carfileHash, struct{}{})
		defer carfileOperation.toDeleteCarfile.Delete(carfileHash)

		_, err := carfileOperation.deleteCarfile(carfileCID)
		if err != nil {
			log.Errorf("DeleteCarfile, delete carfile error:%s, carfileCID:%s", err.Error(), carfileCID)
		}

		blockCount, err := carfileOperation.carfileStore.BlockCount()
		if err == nil {
			carfileOperation.TotalBlockCount = blockCount
		} else {
			log.Errorf("DeleteCarfile, BlockCount error:%s", err.Error())
		}

		_, diskUsage := carfileOperation.device.GetDiskUsageStat()
		info := api.RemoveCarfileResultInfo{BlockCount: carfileOperation.TotalBlockCount, DiskUsage: diskUsage}

		ctx, cancel := context.WithTimeout(context.Background(), helper.SchedulerApiTimeout*time.Second)
		defer cancel()

		err = carfileOperation.scheduler.RemoveCarfileResult(ctx, info)
		if err != nil {
			log.Errorf("DeleteCarfile, RemoveCarfileResult error:%s, carfileCID:%s", err.Error(), carfileCID)
		}

		log.Infof("DeleteCarfile, carfile cid:%s", carfileCID)

	}()
	return nil
}

func (carfileOperation *CarfileOperation) DeleteAllCarfiles(ctx context.Context) error {
	return nil
}

func (carfileOperation *CarfileOperation) LoadBlock(ctx context.Context, cid string) ([]byte, error) {
	blockHash, err := helper.CIDString2HashString(cid)
	if err != nil {
		return nil, err
	}
	return carfileOperation.carfileStore.GetBlock(blockHash)
}

func (carfileOperation *CarfileOperation) QueryCacheStat(ctx context.Context) (*api.CacheStat, error) {
	blockCount, err := carfileOperation.carfileStore.BlockCount()
	if err != nil {
		log.Errorf("QueryCacheStat, block count error:%v", err)
		return nil, nil
	}

	carfileCount, err := carfileOperation.carfileStore.CarfileCount()
	if err != nil {
		log.Errorf("QueryCacheStat, block count error:%v", err)
		return nil, nil
	}

	cacheStat := &api.CacheStat{}
	cacheStat.TotalCarfileCount = carfileCount
	cacheStat.TotalBlockCount = blockCount
	cacheStat.WaitCacheCarfileCount = carfileOperation.downloadMgr.waitListLen()
	_, cacheStat.DiskUsage = carfileOperation.device.GetDiskUsageStat()

	carfileCache := carfileOperation.downloadMgr.getFirstCarfileCacheFromWaitList()
	if carfileCache != nil {
		cacheStat.CachingCarfileCID = carfileCache.carfileCID
	}

	log.Infof("QueryCacheStat, TotalCarfileCount:%d,TotalBlockCount:%d,WaitCacheCarfileCount:%d,DiskUsage:%f,CachingCarfileCID:%s",
		cacheStat.TotalCarfileCount, cacheStat.TotalBlockCount, cacheStat.WaitCacheCarfileCount, cacheStat.DiskUsage, cacheStat.CachingCarfileCID)
	return cacheStat, nil
}

func (carfileOperation *CarfileOperation) QueryCachingCarfile(ctx context.Context) (*api.CachingCarfile, error) {
	carfileCache := carfileOperation.downloadMgr.getFirstCarfileCacheFromWaitList()
	if carfileCache == nil {
		return nil, nil
	}

	ret := &api.CachingCarfile{}
	ret.CarfileCID = carfileCache.carfileCID
	ret.BlockList = carfileCache.blocksWaitList

	return ret, nil
}
