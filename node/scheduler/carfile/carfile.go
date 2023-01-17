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

func (d *CarfileRecord) saveCarfileCacheInfo(cache *CacheTask) error {
	if cache.status == api.CacheStatusSuccess {
		d.reliability += cache.reliability
	}

	dInfo := &api.CarfileRecordInfo{
		CarfileHash: d.carfileHash,
		TotalSize:   d.totalSize,
		TotalBlocks: d.totalBlocks,
		Reliability: d.reliability,
	}

	cInfo := &api.CacheTaskInfo{
		CarfileHash:  cache.carfileHash,
		DeviceID:     cache.deviceID,
		Status:       cache.status,
		DoneSize:     cache.doneSize,
		DoneBlocks:   cache.doneBlocks,
		ExecuteCount: cache.executeCount,
		Reliability:  cache.reliability,
	}

	return persistent.GetDB().SaveCacheResults(dInfo, cInfo)
}

func (d *CarfileRecord) restartUndoneCache() (isRunning bool) {
	isRunning = false

	cacheList := d.getExecutableCaches()
	if len(cacheList) > 0 {
		for _, cache := range cacheList {
			err := cache.startCache()
			if err != nil {
				log.Errorf("startCache %s , node:%s,err:%s", cache.carfileRecord.carfileCid, cache.deviceID, err.Error())
				continue
			}

			isRunning = true
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
	// candidates cache
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

		// err = xerrors.Errorf("candidates not running cache task")
		// return
	}

	if len(d.rootCacheDowloadInfos) <= 0 {
		err = xerrors.New("not found rootCache")
		return
	}

	needCount := d.needReliability - d.reliability
	if needCount <= 0 {
		return
	}

	edges := d.carfileManager.findAppropriateEdges(d.CacheTaskMap, needCount)
	if len(edges) <= 0 {
		err = xerrors.New("not found edge")
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
		// Carfile cache end
		d.carfileManager.removeCarfileRecord(d)
	}

	return nil
}

func (d *CarfileRecord) getExecutableCaches() []*CacheTask {
	// old cache
	list := make([]*CacheTask, 0)

	haveRootCache := len(d.rootCacheDowloadInfos) > 0

	d.CacheTaskMap.Range(func(key, value interface{}) bool {
		c := value.(*CacheTask)

		if c.status == api.CacheStatusSuccess {
			return true
		}

		if c.isRootCache {
			list = append(list, c)
		} else {
			if haveRootCache {
				list = append(list, c)
			}
		}

		return true
	})

	return list
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
