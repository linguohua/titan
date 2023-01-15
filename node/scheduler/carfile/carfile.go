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

	rootCaches   int
	CacheTaskMap sync.Map

	dowloadInfoslock      sync.RWMutex
	rootCacheDowloadInfos []*api.DowloadSource
}

func newCarfileRecord(manager *Manager, cid, hash string) *CarfileRecord {
	return &CarfileRecord{
		nodeManager:           manager.nodeManager,
		carfileManager:        manager,
		carfileCid:            cid,
		reliability:           0,
		totalBlocks:           1,
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

	caches, err := persistent.GetDB().GetCachesWithHash(hash, false)
	if err != nil {
		log.Errorf("loadData hash:%s, GetCachesWithHash err:%s", hash, err.Error())
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
			// expiredTime:   cache.ExpiredTime,
			carfileHash:  cache.CarfileHash,
			executeCount: cache.ExecuteCount,
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

func (d *CarfileRecord) saveCarfileCacheInfo(doneCache *CacheTask) error {
	if doneCache.status == api.CacheStatusSuccess {
		d.reliability += doneCache.reliability
	}

	dInfo := &api.CarfileRecordInfo{
		CarfileHash: d.carfileHash,
		TotalSize:   d.totalSize,
		TotalBlocks: d.totalBlocks,
		Reliability: d.reliability,
	}

	cInfo := &api.CacheTaskInfo{
		CarfileHash:  doneCache.carfileHash,
		DeviceID:     doneCache.deviceID,
		Status:       doneCache.status,
		DoneSize:     doneCache.doneSize,
		DoneBlocks:   doneCache.doneBlocks,
		ExecuteCount: doneCache.executeCount,
		Reliability:  doneCache.reliability,
	}

	return persistent.GetDB().SaveCacheResults(dInfo, cInfo)
	// if err != nil {
	// 	return err
	// }

	// dataTask := &cache.DataTask{CarfileHash: d.carfileHash, DeviceID: doneCache.deviceID}
	// //TODO  doneSize doneBlocks Inaccurate
	// return cache.GetDB().CacheEndRecord(dataTask, "", doneCache.doneSize, doneCache.doneBlocks, isSuccess)
}

func (d *CarfileRecord) restartUndoneCache() (isRuning bool) {
	isRuning = false

	cacheList := d.getUndoneCaches()
	if len(cacheList) > 0 {
		runningList := make([]string, 0)
		// need new cache
		for _, cache := range cacheList {
			err := cache.startCache()
			if err != nil {
				log.Errorf("startCache %s , node:%s,err:%s", cache.carfileRecord.carfileCid, cache.deviceID, err.Error())
				continue
			}

			runningList = append(runningList, cache.deviceID)
			isRuning = true
		}
		// update db
		err := persistent.GetDB().UpdateCacheStatusWithNodes(d.carfileHash, runningList)
		if err != nil {
			log.Errorf("UpdateCacheStatusWithNodes err:%s", err.Error())
		}
	}

	return
}

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

func (d *CarfileRecord) dispatchCache() (isRunning bool, err error) {
	isRunning = d.restartUndoneCache()
	if isRunning {
		return
	}

	needCandidate := d.carfileManager.getNeedRootCacheCount(d.needReliability) - d.rootCaches
	if needCandidate > 0 {
		candidates := d.carfileManager.findAppropriateCandidates(d.CacheTaskMap, needCandidate)
		if len(candidates) <= 0 {
			err = xerrors.New("not found candidate")
			return
		}

		isRunning = d.createAndDoCacheTasks(candidates, true)
		if isRunning {
			return
		}

		err = xerrors.Errorf("candidates not running cache task")
		return
	}

	needCount := d.needReliability - d.reliability
	if needCount <= 0 {
		// caches done
		return
	}

	edges := d.carfileManager.findAppropriateEdges(d.CacheTaskMap, needCount)
	if len(edges) <= 0 {
		err = xerrors.New("not found edge")
		return
	}

	if len(d.rootCacheDowloadInfos) <= 0 {
		err = xerrors.New("not found rootCache")
		return
	}

	// edges cache
	isRunning = d.createAndDoCacheTasks(edges, false)
	if isRunning {
		return
	}

	err = xerrors.Errorf("edges not running cache task")
	return
}

func (d *CarfileRecord) cacheDone(doneCache *CacheTask) error {
	if doneCache.isRootCache && doneCache.status == api.CacheStatusSuccess {
		d.rootCaches++

		cNode := d.nodeManager.GetCandidateNode(doneCache.deviceID)
		if cNode != nil {
			d.dowloadInfoslock.Lock()
			d.rootCacheDowloadInfos = append(d.rootCacheDowloadInfos, &api.DowloadSource{
				CandidateURL:   cNode.GetAddress(),
				CandidateToken: string(d.carfileManager.getAuthToken()),
			})
			d.dowloadInfoslock.Unlock()
		}
	}

	err := d.saveCarfileCacheInfo(doneCache)
	if err != nil {
		return err
	}

	haveRunningTask := false
	d.CacheTaskMap.Range(func(key, value interface{}) bool {
		c := value.(*CacheTask)

		if c.status == api.CacheStatusCreate {
			haveRunningTask = true
			return false
		}

		return true
	})

	if haveRunningTask {
		return nil
	}

	isRunning, err := d.dispatchCache()
	if err != nil {
		log.Errorf("dispatchCache err:%s", err.Error())
	}

	if !isRunning {
		d.carfileManager.removeCarfileRecord(d)
	}

	return nil
}

func (d *CarfileRecord) getUndoneCaches() []*CacheTask {
	// old cache
	oldCache := make([]*CacheTask, 0)
	oldRootCache := make([]*CacheTask, 0)

	d.CacheTaskMap.Range(func(key, value interface{}) bool {
		c := value.(*CacheTask)

		if c.status != api.CacheStatusSuccess && c.executeCount <= 1 {
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

	if len(d.rootCacheDowloadInfos) <= 0 {
		return make([]*CacheTask, 0)
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
