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

// CarfileRecord CarfileRecord
type CarfileRecord struct {
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

	//TODO
	source []*api.DowloadSource
}

func newData(dataManager *Manager, cid, hash string) *CarfileRecord {
	return &CarfileRecord{
		nodeManager: dataManager.nodeManager,
		dataManager: dataManager,
		carfileCid:  cid,
		reliability: 0,
		totalBlocks: 1,
		carfileHash: hash,
		source:      make([]*api.DowloadSource, 0),
	}
}

func loadData(hash string, dataManager *Manager) (*CarfileRecord, error) {
	dInfo, err := persistent.GetDB().GetDataInfo(hash)
	if err != nil {
		return nil, err
	}

	data := &CarfileRecord{}
	data.carfileCid = dInfo.CarfileCid
	data.nodeManager = dataManager.nodeManager
	data.dataManager = dataManager
	data.totalSize = dInfo.TotalSize
	data.needReliability = dInfo.NeedReliability
	data.reliability = dInfo.Reliability
	data.totalBlocks = dInfo.TotalBlocks
	data.expiredTime = dInfo.ExpiredTime
	data.carfileHash = dInfo.CarfileHash
	data.source = make([]*api.DowloadSource, 0)

	caches, err := persistent.GetDB().GetCachesWithData(hash, false)
	if err != nil {
		log.Errorf("loadData hash:%s, GetCachesWithData err:%s", hash, err.Error())
		return data, err
	}

	for _, cache := range caches {
		if cache == nil {
			continue
		}

		c := &CacheTask{
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

			cNode := c.data.nodeManager.GetCandidateNode(c.deviceID)
			if cNode != nil {
				data.source = append(data.source, &api.DowloadSource{
					CandidateURL:   cNode.GetAddress(),
					CandidateToken: string(c.data.dataManager.getAuthToken()),
				})
			}
		}

		data.CacheMap.Store(cache.DeviceID, c)
	}

	return data, nil
}

func (d *CarfileRecord) existRootCache() bool {
	exist := false

	d.CacheMap.Range(func(key, value interface{}) bool {
		if exist {
			return true
		}

		if value != nil {
			c := value.(*CacheTask)
			if c != nil {
				exist = c.isRootCache && c.status == api.CacheStatusSuccess
			}
		}

		return true
	})

	return exist
}

func (d *CarfileRecord) updateAndSaveCacheingInfo(cache *CacheTask) error {
	if !d.existRootCache() {
		d.totalSize = cache.totalSize
		d.totalBlocks = cache.totalBlocks
	}

	dInfo := &api.CarfileRecordInfo{
		CarfileHash: d.carfileHash,
		TotalSize:   d.totalSize,
		TotalBlocks: d.totalBlocks,
		Reliability: d.reliability,
	}

	cInfo := &api.CacheTaskInfo{
		CarfileHash: cache.carfileHash,
		DeviceID:    cache.deviceID,
		DoneSize:    cache.doneSize,
		Status:      cache.status,
		DoneBlocks:  cache.doneBlocks,
	}

	return persistent.GetDB().SaveCacheingResults(dInfo, cInfo)
}

func (d *CarfileRecord) updateAndSaveCacheEndInfo(doneCache *CacheTask) error {
	if doneCache.status == api.CacheStatusSuccess {
		d.reliability += doneCache.reliability

		err := cache.GetDB().IncrByBaseInfo(cache.CarFileCountField, 1)
		if err != nil {
			log.Errorf("updateAndSaveCacheEndInfo IncrByBaseInfo err: %s", err.Error())
		}
	}

	dInfo := &api.CarfileRecordInfo{
		CarfileHash: d.carfileHash,
		TotalSize:   d.totalSize,
		TotalBlocks: d.totalBlocks,
		Reliability: d.reliability,
	}

	cInfo := &api.CacheTaskInfo{
		CarfileHash: doneCache.carfileHash,
		DeviceID:    doneCache.deviceID,
		Status:      doneCache.status,
		DoneSize:    doneCache.doneSize,
		DoneBlocks:  doneCache.doneBlocks,
	}

	return persistent.GetDB().SaveCacheEndResults(dInfo, cInfo)
}

func (d *CarfileRecord) dispatchCache() map[string]string {
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

func (d *CarfileRecord) cacheEnd(doneCache *CacheTask) {
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

func (d *CarfileRecord) getUndoneCache() *CacheTask {
	// old cache
	var oldCache *CacheTask
	var oldRootCache *CacheTask

	d.CacheMap.Range(func(key, value interface{}) bool {
		c := value.(*CacheTask)

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
func (d *CarfileRecord) GetCarfileCid() string {
	return d.carfileCid
}

// GetCarfileHash get carfile hash
func (d *CarfileRecord) GetCarfileHash() string {
	return d.carfileHash
}

// GetTotalSize get total size
func (d *CarfileRecord) GetTotalSize() int {
	return d.totalSize
}

// GetNeedReliability get need reliability
func (d *CarfileRecord) GetNeedReliability() int {
	return d.needReliability
}

// GetReliability get reliability
func (d *CarfileRecord) GetReliability() int {
	return d.reliability
}

// GetTotalBlocks get total blocks
func (d *CarfileRecord) GetTotalBlocks() int {
	return d.totalBlocks
}

// GetTotalNodes get total nodes
func (d *CarfileRecord) GetTotalNodes() int {
	return d.nodes
}
