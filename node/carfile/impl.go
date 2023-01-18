package carfile

import (
	"context"
	"time"

	"github.com/ipfs/go-datastore"
	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/node/helper"
)

func (carfileOperation *CarfileOperation) CacheCarfile(ctx context.Context, carfileCID string, sources []*api.DowloadSource) (api.CacheCarfileResult, error) {
	cfCache := &carfileCache{
		carfileCID:                carfileCID,
		blocksWaitList:            make([]string, 0),
		blocksDownloadSuccessList: make([]string, 0),
		nextLayerCIDs:             make([]string, 0),
		downloadSources:           sources,
	}

	carfileHash, err := helper.CIDString2HashString(carfileCID)
	if err != nil {
		log.Errorf("CacheCarfile, CIDString2HashString error:%s, carfile cid:%s", err.Error(), carfileCID)
		return api.CacheCarfileResult{}, err
	}

	has, err := carfileOperation.carfileStore.HasCarfile(carfileHash)
	if err != nil {
		log.Errorf("CacheCarfile, HasCarfile error:%s, carfile hash :%s", err.Error(), carfileHash)
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
	go func() {
		_, err := carfileOperation.deleteCarfile(carfileCID)
		if err != nil {
			log.Errorf("DeleteCarfile, delete carfile error:%s, carfileCID:%s", err.Error(), carfileCID)
		}

		blockCount, err := carfileOperation.carfileStore.BlockCount()
		if err != nil {
			log.Errorf("DeleteCarfile, BlockCount error:%s", err.Error())
		}

		_, diskUsage := carfileOperation.device.GetDiskUsageStat()
		info := api.RemoveCarfileResultInfo{BlockCount: blockCount, DiskUsage: diskUsage}

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
func (carfileOperation *CarfileOperation) DeleteWaitCacheCarfile(ctx context.Context, carfileCID string) (int, error) {
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

	// TODO:save wait list
	return len(hashs), nil

}

func (carfileOperation *CarfileOperation) LoadBlock(ctx context.Context, cid string) ([]byte, error) {
	blockHash, err := helper.CIDString2HashString(cid)
	if err != nil {
		return nil, err
	}
	return carfileOperation.carfileStore.GetBlock(blockHash)
}
