package carfile

import (
	"sync"
	"time"

	"github.com/linguohua/titan/api"
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
	// nodes           int
	expiredTime time.Time
	rootCaches  int

	CacheTaskMap sync.Map

	dowloadInfoslock      sync.RWMutex
	rootCacheDowloadInfos []*api.DowloadSource
}

func newData(dataManager *Manager, cid, hash string) *CarfileRecord {
	return &CarfileRecord{
		nodeManager:           dataManager.nodeManager,
		dataManager:           dataManager,
		carfileCid:            cid,
		reliability:           0,
		totalBlocks:           1,
		carfileHash:           hash,
		rootCacheDowloadInfos: make([]*api.DowloadSource, 0),
	}
}

func loadCarfileRecord(hash string, dataManager *Manager) (*CarfileRecord, error) {
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
	data.rootCacheDowloadInfos = make([]*api.DowloadSource, 0)

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
			deviceID:      cache.DeviceID,
			carfileRecord: data,
			doneSize:      cache.DoneSize,
			doneBlocks:    cache.DoneBlocks,
			status:        api.CacheStatus(cache.Status),
			isRootCache:   cache.RootCache,
			expiredTime:   cache.ExpiredTime,
			carfileHash:   cache.CarfileHash,
			cacheCount:    cache.CacheCount,
		}

		if c.isRootCache && c.status == api.CacheStatusSuccess {
			data.rootCaches++

			cNode := c.carfileRecord.nodeManager.GetCandidateNode(c.deviceID)
			if cNode != nil {
				data.rootCacheDowloadInfos = append(data.rootCacheDowloadInfos, &api.DowloadSource{
					CandidateURL:   cNode.GetAddress(),
					CandidateToken: string(c.carfileRecord.dataManager.getAuthToken()),
				})
			}
		}

		data.CacheTaskMap.Store(cache.DeviceID, c)
	}

	return data, nil
}

func (d *CarfileRecord) existRootCache() bool {
	exist := false

	d.CacheTaskMap.Range(func(key, value interface{}) bool {
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

// func (d *CarfileRecord) updateAndSaveCacheingInfo(cache *CacheTask) error {
// 	dInfo := &api.CarfileRecordInfo{
// 		CarfileHash: d.carfileHash,
// 		TotalSize:   d.totalSize,
// 		TotalBlocks: d.totalBlocks,
// 	}

// 	cInfo := &api.CacheTaskInfo{
// 		CarfileHash: cache.carfileHash,
// 		DeviceID:    cache.deviceID,
// 		DoneSize:    cache.doneSize,
// 		DoneBlocks:  cache.doneBlocks,
// 	}

// 	return persistent.GetDB().SaveCacheingResults(dInfo, cInfo)
// }

func (d *CarfileRecord) updateAndSaveCacheInfo(doneCache *CacheTask) error {
	// isSuccess := false
	if doneCache.status == api.CacheStatusSuccess {
		d.reliability += doneCache.reliability
		// isSuccess = true
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
		CacheCount:  doneCache.cacheCount,
		Reliability: doneCache.reliability,
	}

	return persistent.GetDB().SaveCacheResults(dInfo, cInfo)
	// if err != nil {
	// 	return err
	// }

	// dataTask := &cache.DataTask{CarfileHash: d.carfileHash, DeviceID: doneCache.deviceID}
	// //TODO  doneSize doneBlocks Inaccurate
	// return cache.GetDB().CacheEndRecord(dataTask, "", doneCache.doneSize, doneCache.doneBlocks, isSuccess)
}

func (d *CarfileRecord) restartUndoneCache() (errorNodes map[string]string, isRuning bool) {
	errorNodes = map[string]string{}
	isRuning = false

	cacheList := d.getUndoneCaches()
	if len(cacheList) > 0 {
		//need new cache
		for _, cache := range cacheList {
			err := cache.startCache()
			if err != nil {
				errorNodes[cache.deviceID] = err.Error()
				continue
			}

			isRuning = true
		}
	}

	return
}

func (d *CarfileRecord) dispatchCache() (errorNodes map[string]string, err error) {
	err = xerrors.New("not running")
	errorNodes, isRunning := d.restartUndoneCache()
	if isRunning {
		return
	}

	needCandidate := d.dataManager.getNeedRootCacheCount(d.needReliability) - d.rootCaches
	if needCandidate > 0 {
		candidates := d.dataManager.findAppropriateCandidates(d.CacheTaskMap, needCandidate)
		if len(candidates) <= 0 {
			err = xerrors.New("not found candidate")
			return
		}

		for _, node := range candidates {
			deviceID := node.DeviceId
			cache, e := newCache(d, node.Node, true)
			if e != nil {
				errorNodes[deviceID] = e.Error()
				continue
			}

			d.CacheTaskMap.Store(deviceID, cache)

			e = cache.startCache()
			if e != nil {
				errorNodes[deviceID] = e.Error()
				continue
			}

			err = nil
		}

		return
	}

	edges := d.dataManager.findAppropriateEdges(d.CacheTaskMap, d.needReliability-d.reliability)
	if len(edges) <= 0 {
		err = xerrors.New("not found edge")
		return
	}

	if len(d.rootCacheDowloadInfos) <= 0 {
		err = xerrors.New("not found rootCache")
		return
	}

	// edges cache
	for _, node := range edges {
		deviceID := node.DeviceId
		cache, e := newCache(d, node.Node, false)
		if e != nil {
			errorNodes[deviceID] = e.Error()
			continue
		}

		d.CacheTaskMap.Store(deviceID, cache)

		e = cache.startCache()
		if e != nil {
			errorNodes[deviceID] = e.Error()
			continue
		}

		err = nil
	}

	return
}

func (d *CarfileRecord) cacheDone(doneCache *CacheTask) error {
	if doneCache.isRootCache {
		d.rootCaches++

		cNode := d.nodeManager.GetCandidateNode(doneCache.deviceID)
		if cNode != nil {
			d.dowloadInfoslock.Lock()
			d.rootCacheDowloadInfos = append(d.rootCacheDowloadInfos, &api.DowloadSource{
				CandidateURL:   cNode.GetAddress(),
				CandidateToken: string(d.dataManager.getAuthToken()),
			})
			d.dowloadInfoslock.Unlock()
		}
	}

	err := d.updateAndSaveCacheInfo(doneCache)
	if err != nil {
		return err
	}

	errList, err := d.dispatchCache()
	if len(errList) > 0 {
		for deviceID, e := range errList {
			log.Errorf("cache deviceID:%s, err:%s", deviceID, e)
			// TODO record node
		}
	}

	if err != nil {
		return err
	}

	d.dataManager.recordTaskEnd(d.carfileCid, d.carfileHash)
	return nil
}

func (d *CarfileRecord) getUndoneCaches() []*CacheTask {
	// old cache
	oldCache := make([]*CacheTask, 0)
	oldRootCache := make([]*CacheTask, 0)

	d.CacheTaskMap.Range(func(key, value interface{}) bool {
		c := value.(*CacheTask)

		if c.status != api.CacheStatusSuccess && c.cacheCount <= 1 {
			if c.isRootCache {
				oldRootCache = append(oldRootCache, c)
			} else {
				oldCache = append(oldCache, c)
			}
		}

		return true
	})

	if len(oldRootCache) > 0 {
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

// // GetTotalNodes get total nodes
// func (d *CarfileRecord) GetTotalNodes() int {
// 	return d.nodes
// }
