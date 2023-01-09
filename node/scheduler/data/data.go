package data

import (
	"sync"
	"time"

	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/node/scheduler/db/cache"
	"github.com/linguohua/titan/node/scheduler/db/persistent"
	"github.com/linguohua/titan/node/scheduler/node"
	"golang.org/x/xerrors"
)

// Data Data
type Data struct {
	nodeManager *node.Manager
	dataManager *Manager

	carfileCid      string
	carfileHash     string
	reliability     int
	needReliability int
	totalSize       int
	totalBlocks     int
	nodes           int
	expiredTime     time.Time
	rootCaches      int

	CacheMap sync.Map

	candidates []string
	edges      []string
}

func newData(dataManager *Manager, cid, hash string) *Data {
	return &Data{
		nodeManager: dataManager.nodeManager,
		dataManager: dataManager,
		carfileCid:  cid,
		reliability: 0,
		totalBlocks: 1,
		carfileHash: hash,
	}
}

func loadData(hash string, dataManager *Manager) (*Data, error) {
	dInfo, err := persistent.GetDB().GetDataInfo(hash)
	if err != nil {
		return nil, err
	}

	data := &Data{}
	data.carfileCid = dInfo.CarfileCid
	data.nodeManager = dataManager.nodeManager
	data.dataManager = dataManager
	data.totalSize = dInfo.TotalSize
	data.needReliability = dInfo.NeedReliability
	data.reliability = dInfo.Reliability
	data.totalBlocks = dInfo.TotalBlocks
	data.expiredTime = dInfo.ExpiredTime
	data.carfileHash = dInfo.CarfileHash

	caches, err := persistent.GetDB().GetCachesWithData(hash, false)
	if err != nil {
		log.Errorf("loadData hash:%s, GetCachesWithData err:%s", hash, err.Error())
		return data, err
	}

	for _, cache := range caches {
		if cache == nil {
			continue
		}

		c := &Cache{
			deviceID:    cache.DeviceID,
			data:        data,
			doneSize:    cache.DoneSize,
			doneBlocks:  cache.DoneBlocks,
			status:      api.CacheStatus(cache.Status),
			isRootCache: cache.RootCache,
			expiredTime: cache.ExpiredTime,
			carfileHash: cache.CarfileHash,
			cacheCount:  cache.CacheCount,
		}

		if c.isRootCache && c.status == api.CacheStatusSuccess {
			data.rootCaches++
		}

		data.CacheMap.Store(cache.DeviceID, c)
	}

	return data, nil
}

func (d *Data) existRootCache() bool {
	exist := false

	d.CacheMap.Range(func(key, value interface{}) bool {
		if exist {
			return true
		}

		if value != nil {
			c := value.(*Cache)
			if c != nil {
				exist = c.isRootCache && c.status == api.CacheStatusSuccess
			}
		}

		return true
	})

	return exist
}

func (d *Data) updateAndSaveCacheingInfo(cache *Cache) error {
	if !d.existRootCache() {
		d.totalSize = cache.totalSize
		d.totalBlocks = cache.totalBlocks
	}

	dInfo := &api.DataInfo{
		CarfileHash: d.carfileHash,
		TotalSize:   d.totalSize,
		TotalBlocks: d.totalBlocks,
		Reliability: d.reliability,
	}

	cInfo := &api.CacheInfo{
		CarfileHash: cache.carfileHash,
		DeviceID:    cache.deviceID,
		DoneSize:    cache.doneSize,
		Status:      cache.status,
		DoneBlocks:  cache.doneBlocks,
	}

	return persistent.GetDB().SaveCacheingResults(dInfo, cInfo)
}

func (d *Data) updateAndSaveCacheEndInfo(doneCache *Cache) error {
	if doneCache.status == api.CacheStatusSuccess {
		d.reliability += doneCache.reliability

		err := cache.GetDB().IncrByBaseInfo(cache.CarFileCountField, 1)
		if err != nil {
			log.Errorf("updateAndSaveCacheEndInfo IncrByBaseInfo err: %s", err.Error())
		}
	}

	dInfo := &api.DataInfo{
		CarfileHash: d.carfileHash,
		TotalSize:   d.totalSize,
		TotalBlocks: d.totalBlocks,
		Reliability: d.reliability,
	}

	cInfo := &api.CacheInfo{
		CarfileHash: doneCache.carfileHash,
		DeviceID:    doneCache.deviceID,
		Status:      doneCache.status,
		DoneSize:    doneCache.doneSize,
		DoneBlocks:  doneCache.doneBlocks,
	}

	return persistent.GetDB().SaveCacheEndResults(dInfo, cInfo)
}

func (d *Data) dispatchCache() map[string]string {
	errorNodes := map[string]string{}

	if len(d.candidates) > 0 {
		for _, deviceID := range d.candidates {
			cache, err := newCache(d, deviceID, true)
			if err != nil {
				errorNodes[deviceID] = err.Error()
				continue
			}

			d.CacheMap.Store(deviceID, cache)

			err = cache.startCache()
			if err != nil {
				errorNodes[deviceID] = err.Error()
				continue
			}
		}

		return errorNodes
	}

	// edge cache
	for _, deviceID := range d.edges {
		cache, err := newCache(d, deviceID, false)
		if err != nil {
			errorNodes[deviceID] = err.Error()
			continue
		}

		d.CacheMap.Store(deviceID, cache)

		err = cache.startCache()
		if err != nil {
			errorNodes[deviceID] = err.Error()
			continue
		}
	}

	return errorNodes
}

func (d *Data) cacheEnd(doneCache *Cache) {
	var err error

	defer func() {
		if err != nil {
			d.dataManager.recordTaskEnd(d.carfileCid, d.carfileHash, err.Error())
		}
	}()

	err = d.updateAndSaveCacheEndInfo(doneCache)
	if err != nil {
		err = xerrors.Errorf("updateAndSaveCacheEndInfo err:%s", err.Error())
		return
	}

	// err = d.dispatchCache(d.getUndoneCache())
}

func (d *Data) getUndoneCache() *Cache {
	// old cache
	var oldCache *Cache
	var oldRootCache *Cache

	d.CacheMap.Range(func(key, value interface{}) bool {
		c := value.(*Cache)

		if c.status != api.CacheStatusSuccess {
			oldCache = c

			if c.isRootCache {
				oldRootCache = c
			}
		}

		return true
	})

	if oldRootCache != nil {
		return oldRootCache
	}

	return oldCache
}

// GetCarfileCid get carfile cid
func (d *Data) GetCarfileCid() string {
	return d.carfileCid
}

// GetCarfileHash get carfile hash
func (d *Data) GetCarfileHash() string {
	return d.carfileHash
}

// GetTotalSize get total size
func (d *Data) GetTotalSize() int {
	return d.totalSize
}

// GetNeedReliability get need reliability
func (d *Data) GetNeedReliability() int {
	return d.needReliability
}

// GetReliability get reliability
func (d *Data) GetReliability() int {
	return d.reliability
}

// GetTotalBlocks get total blocks
func (d *Data) GetTotalBlocks() int {
	return d.totalBlocks
}

// GetTotalNodes get total nodes
func (d *Data) GetTotalNodes() int {
	return d.nodes
}
