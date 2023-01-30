package carfile

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
	nodeManager    *node.Manager
	carfileManager *Manager

	carfileCid      string
	carfileHash     string
	reliability     int
	needReliability int
	totalSize       int64
	totalBlocks     int
	expiredTime     time.Time

	candidateCaches int
	CacheTaskMap    sync.Map

	cacheLock      sync.RWMutex
	dowloadSources []*api.DowloadSource
}

func newCarfileRecord(manager *Manager, cid, hash string) *CarfileRecord {
	return &CarfileRecord{
		nodeManager:    manager.nodeManager,
		carfileManager: manager,
		carfileCid:     cid,
		carfileHash:    hash,
		dowloadSources: make([]*api.DowloadSource, 0),
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
	carfileRecord.dowloadSources = make([]*api.DowloadSource, 0)

	caches, err := persistent.GetDB().GetCaches(hash, false)
	if err != nil {
		log.Errorf("loadData hash:%s, GetCaches err:%s", hash, err.Error())
		return carfileRecord, err
	}

	for _, cacheInfo := range caches {
		if cacheInfo == nil {
			continue
		}

		c := &CacheTask{
			id:               cacheInfo.ID,
			deviceID:         cacheInfo.DeviceID,
			carfileRecord:    carfileRecord,
			doneSize:         cacheInfo.DoneSize,
			doneBlocks:       cacheInfo.DoneBlocks,
			status:           cacheInfo.Status,
			isCandidateCache: cacheInfo.CandidateCache,
			carfileHash:      cacheInfo.CarfileHash,
		}

		if c.isCandidateCache && c.status == api.CacheStatusSuccess {
			carfileRecord.candidateCaches++

			cNode := c.carfileRecord.nodeManager.GetCandidateNode(c.deviceID)
			if cNode != nil {
				carfileRecord.dowloadSources = append(carfileRecord.dowloadSources, &api.DowloadSource{
					CandidateURL:   cNode.GetAddress(),
					CandidateToken: string(carfileRecord.carfileManager.authToken),
				})
			}
		}

		carfileRecord.CacheTaskMap.Store(cacheInfo.DeviceID, c)
	}

	return carfileRecord, nil
}

func (d *CarfileRecord) candidateCacheExists() bool {
	exist := false

	d.CacheTaskMap.Range(func(key, value interface{}) bool {
		if value == nil {
			return true
		}

		c := value.(*CacheTask)
		if c == nil {
			return true
		}

		exist = c.isCandidateCache && c.status == api.CacheStatusSuccess
		if exist {
			return false
		}

		return true
	})

	return exist
}

func (d *CarfileRecord) createAndDoCacheTasks(nodes []*node.Node, isCandidate bool) (isRunning bool) {
	isRunning = false

	runningList := make([]string, 0)
	errorList := make([]string, 0)

	for _, node := range nodes {
		deviceID := node.DeviceId
		c, err := newCache(d, deviceID, isCandidate)
		if err != nil {
			log.Errorf("newCache %s , node:%s,err:%s", d.carfileCid, deviceID, err.Error())
			continue
		}

		d.CacheTaskMap.Store(deviceID, c)

		runningList = append(runningList, deviceID)
	}

	if len(runningList) < 1 {
		return
	}

	err := cache.GetDB().CacheTasksStart(d.carfileHash, runningList, nodeCacheTimeout)
	if err != nil {
		log.Errorf("createAndDoCacheTasks %s , CacheTasksStart err:%s", d.carfileHash, err.Error())
		return
	}

	// set cache status
	err = persistent.GetDB().UpdateCacheTaskStatus(d.carfileHash, runningList, api.CacheStatusRunning)
	if err != nil {
		log.Errorf("createAndDoCacheTasks %s , UpdateCacheTaskStatus err:%s", d.carfileHash, err.Error())
	}

	for _, deviceID := range runningList {
		cI, exist := d.CacheTaskMap.Load(deviceID)
		if exist && cI != nil {
			c := cI.(*CacheTask)
			err = c.startCache()
			if err == nil {
				isRunning = true
				continue
			}
			log.Errorf("startCache %s , node:%s,err:%s", c.carfileRecord.carfileCid, c.deviceID, err.Error())
		}
		errorList = append(errorList, deviceID)
	}

	if len(errorList) > 0 {
		// set cache status
		err = persistent.GetDB().UpdateCacheTaskStatus(d.carfileHash, errorList, api.CacheStatusFail)
		if err != nil {
			log.Errorf("createAndDoCacheTasks %s , UpdateCacheTaskStatus err:%s", d.carfileHash, err.Error())
		}

		_, err := cache.GetDB().CacheTasksEnd(d.carfileHash, errorList, nil)
		if err != nil {
			log.Errorf("createAndDoCacheTasks %s , CacheTasksEnd err:%s", d.carfileHash, err.Error())
			return
		}
	}

	return
}

func (d *CarfileRecord) updateCacheTaskStatus(deviceIDs []string, status api.CacheStatus) error {
	// update cache info to db
	return persistent.GetDB().UpdateCacheTaskStatus(d.carfileHash, deviceIDs, status)
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
	if len(d.dowloadSources) <= 0 {
		return xerrors.New("not found cache sources")
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

func (d *CarfileRecord) dispatchCaches() (isRunning bool, err error) {
	needCacdidateCount := d.carfileManager.rootCacheCount
	if d.candidateCaches > 0 {
		needCacdidateCount = (d.carfileManager.rootCacheCount + d.carfileManager.backupCacheCount) - d.candidateCaches
	}
	if needCacdidateCount > 0 {
		err = d.cacheToCandidates(needCacdidateCount)
		if err == nil {
			// cache to candidates
			isRunning = true
		}
		// log.Errorf("%s cacheToCandidates err:%s", d.carfileCid, err.Error())
		return
	}

	needEdgeCount := d.needReliability - d.reliability
	if needEdgeCount <= 0 {
		// no caching required
		return
	}

	err = d.cacheToEdges(needEdgeCount)
	if err == nil {
		// cache to edges
		isRunning = true
	}

	return
}

func (d *CarfileRecord) cacheDone(endCache *CacheTask, cachesDone bool) error {
	if endCache.status == api.CacheStatusSuccess {
		d.cacheLock.Lock()
		d.reliability += endCache.reliability

		if endCache.isCandidateCache {
			d.candidateCaches++

			cNode := d.nodeManager.GetCandidateNode(endCache.deviceID)
			if cNode != nil {
				d.dowloadSources = append(d.dowloadSources, &api.DowloadSource{
					CandidateURL:   cNode.GetAddress(),
					CandidateToken: string(d.carfileManager.authToken),
				})
			}
		}
		d.cacheLock.Unlock()
	}

	if !cachesDone {
		// caches undone
		return nil
	}

	//change carfile phase

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

			d.carfileManager.carfileCacheEnd(d)
		}
	}()

	if endCache.isCandidateCache {
		//
		isRunning, err := d.dispatchCaches()
		if err != nil {
			log.Errorf("dispatchCaches err:%s", err.Error())
		}

		ended = !isRunning
	}

	return nil
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
