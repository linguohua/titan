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
	nodeManager    *node.Manager
	carfileManager *Manager

	carfileCid      string
	carfileHash     string
	reliability     int
	needReliability int
	totalSize       int64
	totalBlocks     int
	expiredTime     time.Time

	needRootCaches int
	rootCaches     int
	CacheTaskMap   sync.Map

	cacheLock             sync.RWMutex
	rootCacheDowloadInfos []*api.DowloadSource
}

func newCarfileRecord(manager *Manager, cid, hash string) *CarfileRecord {
	return &CarfileRecord{
		nodeManager:           manager.nodeManager,
		carfileManager:        manager,
		carfileCid:            cid,
		carfileHash:           hash,
		rootCacheDowloadInfos: make([]*api.DowloadSource, 0),
	}
}

func loadCarfileRecord(hash string, manager *Manager) (*CarfileRecord, error) {
	dInfo, err := persistent.GetDB().GetCarfileInfo(hash)
	if err != nil {
		return nil, err
	}

	carfileRecord := &CarfileRecord{}
	carfileRecord.carfileCid = dInfo.CarfileCid
	carfileRecord.nodeManager = manager.nodeManager
	carfileRecord.carfileManager = manager
	carfileRecord.totalSize = dInfo.TotalSize
	carfileRecord.needReliability = dInfo.NeedReliability
	carfileRecord.reliability = dInfo.Reliability
	carfileRecord.totalBlocks = dInfo.TotalBlocks
	carfileRecord.expiredTime = dInfo.ExpiredTime
	carfileRecord.carfileHash = dInfo.CarfileHash
	carfileRecord.rootCacheDowloadInfos = make([]*api.DowloadSource, 0)
	carfileRecord.needRootCaches = manager.needRootCacheCount(carfileRecord.needReliability)

	caches, err := persistent.GetDB().GetCaches(hash, false)
	if err != nil {
		log.Errorf("loadData hash:%s, GetCaches err:%s", hash, err.Error())
		return carfileRecord, err
	}

	for _, cache := range caches {
		if cache == nil {
			continue
		}

		c := &CacheTask{
			deviceID:      cache.DeviceID,
			carfileRecord: carfileRecord,
			doneSize:      cache.DoneSize,
			doneBlocks:    cache.DoneBlocks,
			status:        api.CacheStatus(cache.Status),
			isRootCache:   cache.RootCache,
			carfileHash:   cache.CarfileHash,
			executeCount:  cache.ExecuteCount,
		}

		if c.isRootCache && c.status == api.CacheStatusSuccess {
			carfileRecord.rootCaches++

			cNode := c.carfileRecord.nodeManager.GetCandidateNode(c.deviceID)
			if cNode != nil {
				carfileRecord.rootCacheDowloadInfos = append(carfileRecord.rootCacheDowloadInfos, &api.DowloadSource{
					CandidateURL:   cNode.GetAddress(),
					CandidateToken: string(c.carfileRecord.carfileManager.getAuthToken()),
				})
			}
		}

		carfileRecord.CacheTaskMap.Store(cache.DeviceID, c)
	}

	return carfileRecord, nil
}

func (d *CarfileRecord) rootCacheExists() bool {
	exist := false

	d.CacheTaskMap.Range(func(key, value interface{}) bool {
		if value == nil {
			return true
		}

		c := value.(*CacheTask)
		if c == nil {
			return true
		}

		exist = c.isRootCache && c.status == api.CacheStatusSuccess
		if exist {
			return false
		}

		return true
	})

	return exist
}

// func (d *CarfileRecord) saveCarfileCacheInfo(cache *CacheTask) error {
// 	dInfo := &api.CarfileRecordInfo{
// 		CarfileHash: d.carfileHash,
// 		TotalSize:   d.totalSize,
// 		TotalBlocks: d.totalBlocks,
// 		Reliability: d.reliability,
// 	}

// 	cInfo := &api.CacheTaskInfo{
// 		CarfileHash:  cache.carfileHash,
// 		DeviceID:     cache.deviceID,
// 		Status:       cache.status,
// 		DoneSize:     cache.doneSize,
// 		DoneBlocks:   cache.doneBlocks,
// 		ExecuteCount: cache.executeCount,
// 		Reliability:  cache.reliability,
// 	}

// 	return persistent.GetDB().SaveCacheResults(dInfo, cInfo)
// }

// func (d *CarfileRecord) restartUndoneCache() (isRunning bool) {
// 	isRunning = false

// 	cacheList := d.getExecutableCaches()
// 	if len(cacheList) > 0 {
// 		for _, cache := range cacheList {
// 			err := cache.startCache()
// 			if err != nil {
// 				log.Errorf("startCache %s , node:%s,err:%s", cache.carfileRecord.carfileCid, cache.deviceID, err.Error())
// 				continue
// 			}

// 			isRunning = true
// 		}
// 	}

// 	return
// }

func (d *CarfileRecord) createAndDoCacheTasks(nodes []*node.Node, isRootCache bool) (isRunning bool) {
	isRunning = false

	for _, node := range nodes {
		deviceID := node.DeviceId
		cache, err := newCache(d, deviceID, isRootCache)
		if err != nil {
			log.Errorf("newCache %s , node:%s,err:%s", cache.carfileRecord.carfileCid, cache.deviceID, err.Error())
			continue
		}

		d.CacheTaskMap.Store(deviceID, cache)

		err = cache.startCache()
		if err != nil {
			log.Errorf("startCache %s , node:%s,err:%s", cache.carfileRecord.carfileCid, cache.deviceID, err.Error())
			continue
		}

		isRunning = true
	}

	return
}

func (d *CarfileRecord) cacheToCandidates(needCount int) error {
	candidates := d.carfileManager.findAppropriateCandidates(d.CacheTaskMap, needCount)
	if len(candidates) <= 0 {
		return xerrors.New("not found candidate")
	}

	if !d.createAndDoCacheTasks(candidates, true) {
		return xerrors.New("running err")
	}

	return nil
}

func (d *CarfileRecord) cacheToEdges(needCount int) error {
	if len(d.rootCacheDowloadInfos) <= 0 {
		return xerrors.New("not found rootCache")
	}

	edges := d.carfileManager.findAppropriateEdges(d.CacheTaskMap, needCount)
	if len(edges) <= 0 {
		return xerrors.New("not found edge")
	}

	if !d.createAndDoCacheTasks(edges, false) {
		return xerrors.New("running err")
	}

	return nil
}

// func (d *CarfileRecord) dispatchCache() (isRunning bool, err error) {

// 	// candidates cache
// 	needCandidateCount := d.needRootCaches - d.rootCaches
// 	if needCandidateCount > 0 {
// 		err = d.cacheToCandidates(needCandidateCount)
// 		if err != nil {
// 			return
// 		}

// 		return true, nil
// 	}

// 	if d.requestedEdge {
// 		return
// 	}

// 	//edge cache
// 	needEdgeCount := d.needReliability - d.reliability
// 	if needEdgeCount <= 0 {
// 		// no caching required
// 		return
// 	}

// 	if len(d.rootCacheDowloadInfos) <= 0 {
// 		err = xerrors.New("not found rootCache")
// 		return
// 	}

// 	err = d.cacheToEdges(needEdgeCount)
// 	if err != nil {
// 		return
// 	}

// 	return true, nil
// }

func (d *CarfileRecord) cacheDone(endCache *CacheTask, cachesDone bool) error {
	if endCache.status == api.CacheStatusSuccess {
		d.cacheLock.Lock()
		d.reliability += endCache.reliability

		if endCache.isRootCache {
			d.rootCaches++

			cNode := d.nodeManager.GetCandidateNode(endCache.deviceID)
			if cNode != nil {
				d.rootCacheDowloadInfos = append(d.rootCacheDowloadInfos, &api.DowloadSource{
					CandidateURL:   cNode.GetAddress(),
					CandidateToken: string(d.carfileManager.getAuthToken()),
				})
			}
		}
		d.cacheLock.Unlock()
	}

	// count, err := cache.GetDB().CarfileRunningCount(d.carfileHash)
	// if err != nil {
	// 	log.Errorf("CarfileRunningCount err:%s", err.Error())
	// 	return err
	// }

	if !cachesDone {
		// carfile undone
		return nil
	}

	ended := true
	defer func() {
		if ended {
			// Carfile caches end
			dInfo := &api.CarfileRecordInfo{
				CarfileHash: d.carfileHash,
				TotalSize:   d.totalSize,
				TotalBlocks: d.totalBlocks,
				Reliability: d.reliability,
			}
			err := persistent.GetDB().UpdateCarfileRecordCachesInfo(dInfo)
			if err != nil {
				log.Errorf("UpdateCarfileRecordCachesInfo err:%s", err.Error())
			}

			d.carfileManager.removeCarfileRecord(d)
		}
	}()

	if endCache.isRootCache {
		//cache to edges
		needCount := d.needReliability - d.reliability
		if needCount <= 0 {
			// no caching required
			return nil
		}

		err := d.cacheToEdges(needCount)
		if err != nil {
			log.Errorf("cacheToEdges err:%s", err.Error())
		} else {
			ended = false
		}
	}

	return nil
}

// func (d *CarfileRecord) getExecutableCaches() []*CacheTask {
// 	// old cache
// 	list := make([]*CacheTask, 0)

// 	haveRootCache := len(d.rootCacheDowloadInfos) > 0

// 	d.CacheTaskMap.Range(func(key, value interface{}) bool {
// 		c := value.(*CacheTask)

// 		if c.status == api.CacheStatusSuccess {
// 			return true
// 		}

// 		if c.isRootCache {
// 			list = append(list, c)
// 		} else {
// 			if haveRootCache {
// 				list = append(list, c)
// 			}
// 		}

// 		return true
// 	})

// 	return list
// }

// GetCarfileCid get carfile cid
func (d *CarfileRecord) GetCarfileCid() string {
	return d.carfileCid
}

// GetCarfileHash get carfile hash
func (d *CarfileRecord) GetCarfileHash() string {
	return d.carfileHash
}

// GetTotalSize get total size
func (d *CarfileRecord) GetTotalSize() int64 {
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
