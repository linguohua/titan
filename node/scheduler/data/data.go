package data

import (
	"sync"
	"time"

	"github.com/linguohua/titan/node/scheduler/db/persistent"
	"github.com/linguohua/titan/node/scheduler/node"
	"golang.org/x/xerrors"
)

// Data Data
type Data struct {
	Manager         *node.Manager
	DataManager     *DataManager
	CarfileCid      string
	CarfileHash     string
	CacheMap        sync.Map
	Reliability     int
	NeedReliability int
	CacheCount      int
	TotalSize       int
	TotalBlocks     int
	Nodes           int
	ExpiredTime     time.Time

	IsStop bool
}

func newData(nodeManager *node.Manager, dataManager *DataManager, cid, hash string, reliability int) *Data {
	return &Data{
		Manager:         nodeManager,
		DataManager:     dataManager,
		CarfileCid:      cid,
		Reliability:     0,
		NeedReliability: reliability,
		CacheCount:      0,
		TotalBlocks:     1,
		CarfileHash:     hash,
	}
}

func loadData(hash string, nodeManager *node.Manager, dataManager *DataManager) *Data {
	dInfo, err := persistent.GetDB().GetDataInfo(hash)
	if err != nil && !persistent.GetDB().IsNilErr(err) {
		log.Errorf("loadData %s err :%s", hash, err.Error())
		return nil
	}
	if dInfo != nil {
		data := &Data{}
		data.CarfileCid = dInfo.CarfileCid
		data.Manager = nodeManager
		data.DataManager = dataManager
		data.TotalSize = dInfo.TotalSize
		data.NeedReliability = dInfo.NeedReliability
		data.Reliability = dInfo.Reliability
		data.CacheCount = dInfo.CacheCount
		data.TotalBlocks = dInfo.TotalBlocks
		data.Nodes = dInfo.Nodes
		data.ExpiredTime = dInfo.ExpiredTime
		data.CarfileHash = dInfo.CarfileHash

		idList, err := persistent.GetDB().GetCachesWithData(hash)
		if err != nil {
			log.Errorf("loadData hash:%s, GetCachesWithData err:%s", hash, err.Error())
			return data
		}

		for _, cacheID := range idList {
			if cacheID == "" {
				continue
			}
			c := loadCache(cacheID, hash, nodeManager, data)
			if c == nil {
				continue
			}

			data.CacheMap.Store(cacheID, c)
		}

		return data
	}

	return nil
}

func (d *Data) haveRootCache() bool {
	have := false

	d.CacheMap.Range(func(key, value interface{}) bool {
		if have {
			return true
		}

		if value != nil {
			c := value.(*Cache)
			if c != nil {
				have = c.isRootCache && c.Status == persistent.CacheStatusSuccess
			}
		}

		return true
	})

	return have
}

func (d *Data) createCache(isRootCache bool) (*Cache, error) {
	cache, err := newCache(d.Manager, d, d.CarfileHash, isRootCache)
	if err != nil {
		return nil, xerrors.Errorf("new cache err:%s", err.Error())
	}

	return cache, nil
}

func (d *Data) updateAndSaveCacheingInfo(blockInfo *persistent.BlockInfo, cache *Cache, createBlocks []*persistent.BlockInfo) error {
	if !d.haveRootCache() {
		d.TotalSize = cache.totalSize
		d.TotalBlocks = cache.totalBlocks
	}

	dInfo := &persistent.DataInfo{
		CarfileHash: d.CarfileHash,
		TotalSize:   d.TotalSize,
		TotalBlocks: d.TotalBlocks,
		Reliability: d.Reliability,
		CacheCount:  d.CacheCount,
	}

	cInfo := &persistent.CacheInfo{
		// ID:          cache.dbID,
		CarfileHash: cache.carfileHash,
		CacheID:     cache.CacheID,
		DoneSize:    cache.DoneSize,
		Status:      int(cache.Status),
		DoneBlocks:  cache.DoneBlocks,
		Reliability: cache.reliability,
		TotalSize:   cache.totalSize,
		TotalBlocks: cache.totalBlocks,
	}

	return persistent.GetDB().SaveCacheingResults(dInfo, cInfo, blockInfo, createBlocks)
}

func (d *Data) updateAndSaveCacheEndInfo(cache *Cache) error {
	if cache.Status == persistent.CacheStatusSuccess {
		d.Reliability += cache.reliability
	}

	cNodes, err := persistent.GetDB().GetNodesFromCache(cache.CacheID)
	if err != nil {
		log.Warnf("updateAndSaveCacheEndInfo GetNodesFromCache err:%s", err.Error())
	}

	dNodes, err := persistent.GetDB().GetNodesFromData(d.CarfileHash)
	if err != nil {
		log.Warnf("updateAndSaveCacheEndInfo GetNodesFromData err:%s", err.Error())
	}

	d.Nodes = dNodes
	dInfo := &persistent.DataInfo{
		CarfileHash: d.CarfileHash,
		TotalSize:   d.TotalSize,
		TotalBlocks: d.TotalBlocks,
		Reliability: d.Reliability,
		CacheCount:  d.CacheCount,
		Nodes:       d.Nodes,
	}

	cache.Nodes = cNodes
	cInfo := &persistent.CacheInfo{
		CarfileHash: cache.carfileHash,
		CacheID:     cache.CacheID,
		DoneSize:    cache.DoneSize,
		Status:      int(cache.Status),
		DoneBlocks:  cache.DoneBlocks,
		Reliability: cache.reliability,
		TotalSize:   cache.totalSize,
		TotalBlocks: cache.totalBlocks,
		Nodes:       cache.Nodes,
	}

	return persistent.GetDB().SaveCacheEndResults(dInfo, cInfo)
}

func (d *Data) dispatchCache(cache *Cache) (*Cache, error) {
	var err error
	var list map[string]string

	if cache != nil {
		list, err = persistent.GetDB().GetUndoneBlocks(cache.CacheID)
		if err != nil {
			return cache, err
		}
	} else {
		cache, err = d.createCache(!d.haveRootCache())
		if err != nil {
			return nil, err
		}

		d.CacheMap.Store(cache.CacheID, cache)

		list = map[string]string{d.CarfileCid: ""}
	}

	d.CacheCount++

	err = cache.startCache(list)
	if err != nil {
		return cache, err
	}

	return cache, nil
}

func (d *Data) cacheEnd(doneCache *Cache) {
	var err error

	defer func() {
		if err != nil {
			d.DataManager.dataTaskEnd(d.CarfileCid, d.CarfileHash, err.Error())
		}
	}()

	err = d.updateAndSaveCacheEndInfo(doneCache)
	if err != nil {
		err = xerrors.Errorf("updateAndSaveCacheEndInfo err:%s", err.Error())
		return
	}

	if d.CacheCount > d.NeedReliability {
		err = xerrors.Errorf("cacheCount:%d reach needReliability:%d", d.CacheCount, d.NeedReliability)
		return
	}

	if d.NeedReliability <= d.Reliability {
		err = xerrors.Errorf("reliability is enough:%d/%d", d.Reliability, d.NeedReliability)
		return
	}

	// old cache
	var oldCache *Cache
	d.CacheMap.Range(func(key, value interface{}) bool {
		c := value.(*Cache)

		if c.Status != persistent.CacheStatusSuccess {
			oldCache = c
		}

		return true
	})

	_, err = d.dispatchCache(oldCache)
}
