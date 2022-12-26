package data

import (
	"sync"
	"time"

	"github.com/linguohua/titan/api"
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
	cacheMap        sync.Map
	reliability     int
	needReliability int
	cacheCount      int
	totalSize       int
	totalBlocks     int
	nodes           int
	expiredTime     time.Time
}

func newData(nodeManager *node.Manager, dataManager *Manager, cid, hash string, reliability int) *Data {
	return &Data{
		nodeManager:     nodeManager,
		dataManager:     dataManager,
		carfileCid:      cid,
		reliability:     0,
		needReliability: reliability,
		cacheCount:      0,
		totalBlocks:     1,
		carfileHash:     hash,
	}
}

func loadData(hash string, nodeManager *node.Manager, dataManager *Manager) *Data {
	dInfo, err := persistent.GetDB().GetDataInfo(hash)
	if err != nil && !persistent.GetDB().IsNilErr(err) {
		log.Errorf("loadData %s err :%s", hash, err.Error())
		return nil
	}
	if dInfo != nil {
		data := &Data{}
		data.carfileCid = dInfo.CarfileCid
		data.nodeManager = nodeManager
		data.dataManager = dataManager
		data.totalSize = dInfo.TotalSize
		data.needReliability = dInfo.NeedReliability
		data.reliability = dInfo.Reliability
		data.cacheCount = dInfo.CacheCount
		data.totalBlocks = dInfo.TotalBlocks
		data.nodes = dInfo.Nodes
		data.expiredTime = dInfo.ExpiredTime
		data.carfileHash = dInfo.CarfileHash

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

			data.cacheMap.Store(cacheID, c)
		}

		return data
	}

	return nil
}

func (d *Data) haveRootCache() bool {
	have := false

	d.cacheMap.Range(func(key, value interface{}) bool {
		if have {
			return true
		}

		if value != nil {
			c := value.(*Cache)
			if c != nil {
				have = c.isRootCache && c.status == api.CacheStatusSuccess
			}
		}

		return true
	})

	return have
}

func (d *Data) updateAndSaveCacheingInfo(blockInfo *api.BlockInfo, cache *Cache, createBlocks []*api.BlockInfo) error {
	if !d.haveRootCache() {
		d.totalSize = cache.totalSize
		d.totalBlocks = cache.totalBlocks
	}

	dInfo := &api.DataInfo{
		CarfileHash: d.carfileHash,
		TotalSize:   d.totalSize,
		TotalBlocks: d.totalBlocks,
		Reliability: d.reliability,
		CacheCount:  d.cacheCount,
	}

	cInfo := &api.CacheInfo{
		// ID:          cache.dbID,
		CarfileHash: cache.carfileHash,
		CacheID:     cache.cacheID,
		DoneSize:    cache.doneSize,
		Status:      cache.status,
		DoneBlocks:  cache.doneBlocks,
		Reliability: cache.reliability,
		TotalSize:   cache.totalSize,
		TotalBlocks: cache.totalBlocks,
	}

	return persistent.GetDB().SaveCacheingResults(dInfo, cInfo, blockInfo, createBlocks)
}

func (d *Data) updateAndSaveCacheEndInfo(cache *Cache) error {
	if cache.status == api.CacheStatusSuccess {
		d.reliability += cache.reliability
	}

	// TODO how to merge this two
	cNodes, err := persistent.GetDB().GetNodesFromCache(cache.cacheID)
	if err != nil {
		log.Errorf("updateAndSaveCacheEndInfo GetNodesFromCache err:%s", err.Error())
	}

	dNodes, err := persistent.GetDB().GetNodesFromData(d.carfileHash)
	if err != nil {
		log.Errorf("updateAndSaveCacheEndInfo GetNodesFromData err:%s", err.Error())
	}

	d.nodes = len(dNodes)
	dInfo := &api.DataInfo{
		CarfileHash: d.carfileHash,
		TotalSize:   d.totalSize,
		TotalBlocks: d.totalBlocks,
		Reliability: d.reliability,
		CacheCount:  d.cacheCount,
		Nodes:       d.nodes,
	}

	cache.nodes = len(cNodes)
	cInfo := &api.CacheInfo{
		CarfileHash: cache.carfileHash,
		CacheID:     cache.cacheID,
		DoneSize:    cache.doneSize,
		Status:      cache.status,
		DoneBlocks:  cache.doneBlocks,
		Reliability: cache.reliability,
		TotalSize:   cache.totalSize,
		TotalBlocks: cache.totalBlocks,
		Nodes:       cache.nodes,
	}

	return persistent.GetDB().SaveCacheEndResults(dInfo, cInfo)
}

func (d *Data) dispatchCache(cache *Cache) error {
	var err error
	var list map[string]string

	if cache != nil {
		cache.updateAlreadyMap()

		list, err = persistent.GetDB().GetUndoneBlocks(cache.cacheID)
		if err != nil {
			return err
		}

	} else {
		var blockID string
		cache, blockID, err = newCache(d, !d.haveRootCache())
		if err != nil {
			return err
		}

		d.cacheMap.Store(cache.cacheID, cache)

		list = map[string]string{d.carfileCid: blockID}
	}

	d.cacheCount++

	err = cache.startCache(list)
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

	if !isContinue {
		err = xerrors.Errorf("do not continue")
		return
	}

	if d.cacheCount > d.needReliability {
		err = xerrors.Errorf("cacheCount:%d reach needReliability:%d", d.cacheCount, d.needReliability)
		return
	}

	if d.needReliability <= d.reliability {
		err = xerrors.Errorf("reliability is enough:%d/%d", d.reliability, d.needReliability)
		return
	}

	err = d.dispatchCache(d.getUndoneCache())
}

func (d *Data) getUndoneCache() *Cache {
	// old cache
	var oldCache *Cache
	var oldRootCache *Cache

	d.cacheMap.Range(func(key, value interface{}) bool {
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

// GetCacheMap get cache map
func (d *Data) GetCacheMap() sync.Map {
	return d.cacheMap
}
