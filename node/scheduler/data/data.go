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

	CacheMap sync.Map
}

func newData(nodeManager *node.Manager, dataManager *Manager, cid, hash string, reliability int) *Data {
	return &Data{
		nodeManager:     nodeManager,
		dataManager:     dataManager,
		carfileCid:      cid,
		reliability:     0,
		needReliability: reliability,
		totalBlocks:     1,
		carfileHash:     hash,
		// CacheMap:        new(sync.Map),
	}
}

func loadData(hash string, dataManager *Manager) *Data {
	dInfo, err := persistent.GetDB().GetDataInfo(hash)
	if err != nil && !persistent.GetDB().IsNilErr(err) {
		log.Errorf("loadData %s err :%s", hash, err.Error())
		return nil
	}
	if dInfo != nil {
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
		// data.CacheMap = new(sync.Map)

		caches, err := persistent.GetDB().GetCachesWithData(hash, false)
		if err != nil {
			log.Errorf("loadData hash:%s, GetCachesWithData err:%s", hash, err.Error())
			return data
		}

		for _, cache := range caches {
			if cache == nil {
				continue
			}
			c := loadCache(cache, data)
			if c == nil {
				continue
			}

			data.CacheMap.Store(cache.DeviceID, c)
		}

		return data
	}

	return nil
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
		// ID:          cache.dbID,
		CarfileHash: cache.carfileHash,
		DeviceID:    cache.deviceID,
		DoneSize:    cache.doneSize,
		Status:      cache.status,
		DoneBlocks:  cache.doneBlocks,
	}

	return persistent.GetDB().SaveCacheingResults(dInfo, cInfo)
}

func (d *Data) updateNodeDiskUsage(nodes []string) {
	values := make(map[string]interface{})

	for _, deviceID := range nodes {
		e := d.nodeManager.GetEdgeNode(deviceID)
		if e != nil {
			values[e.DeviceId] = e.DiskUsage
			continue
		}

		c := d.nodeManager.GetCandidateNode(deviceID)
		if c != nil {
			values[c.DeviceId] = c.DiskUsage
			continue
		}
	}

	err := cache.GetDB().UpdateDevicesInfo(cache.DiskUsageField, values)
	if err != nil {
		log.Errorf("updateNodeDiskUsage err:%s", err.Error())
	}
}

func (d *Data) updateAndSaveCacheEndInfo(doneCache *Cache) error {
	if doneCache.status == api.CacheStatusSuccess {
		d.reliability += doneCache.reliability

		err := cache.GetDB().IncrByBaseInfo(cache.CarFileCountField, 1)
		if err != nil {
			log.Errorf("updateAndSaveCacheEndInfo IncrByBaseInfo err: %s", err.Error())
		}
	}

	d.updateNodeDiskUsage([]string{doneCache.deviceID})

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
	}

	return persistent.GetDB().SaveCacheEndResults(dInfo, cInfo)
}

func (d *Data) dispatchCache(cache *Cache) error {
	var err error

	if cache == nil {
		cache, err = newCache(d, !d.existRootCache())
		if err != nil {
			return err
		}

		d.CacheMap.Store(cache.deviceID, cache)

	}

	err = cache.startCache()
	if err != nil {
		return err
	}

	return nil
}

func (d *Data) cacheEnd(doneCache *Cache, isContinue bool) {
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

	err = d.dispatchCache(d.getUndoneCache())
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
