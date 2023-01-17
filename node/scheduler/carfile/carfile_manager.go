package carfile

import (
	"context"
	"sort"
	"sync"
	"time"

	logging "github.com/ipfs/go-log/v2"
	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/node/helper"
	"github.com/linguohua/titan/node/scheduler/db/cache"
	"github.com/linguohua/titan/node/scheduler/db/persistent"
	"github.com/linguohua/titan/node/scheduler/node"
	"golang.org/x/xerrors"
)

var log = logging.Logger("carfile")

const (
	checkCacheTimeoutInterval = 65      //  set to check node cache timeout timer (Unit:Second)
	nodeCacheTimeout          = 70      //  expiration set to redis (Unit:Second)
	startTaskInterval         = 10      //  time interval (Unit:Second)
	checkExpiredTimerInterval = 60 * 30 //  time interval (Unit:Second)

	// It needs to be changed to the number of caches
	runningCarfileMaxCount = 10

	// If the node disk size is greater than this value, caching will not continue
	diskUsageMax = 90.0
)

// Manager carfile
type Manager struct {
	nodeManager       *node.Manager
	CarfileRecordMap  sync.Map // cacheing carfile map
	latelyExpiredTime time.Time
	// isLoadExpiredTime  bool
	getAuthToken     func() []byte
	runningTaskCount int
}

// NewCarfileManager new
func NewCarfileManager(nodeManager *node.Manager, getToken func() []byte) *Manager {
	d := &Manager{
		nodeManager: nodeManager,
		// isLoadExpiredTime: true,
		latelyExpiredTime: time.Now(),
		getAuthToken:      getToken,
	}

	d.initCarfileMap()
	d.resetBaseInfo()
	go d.cacheTaskTicker()
	go d.checkExpiredTicker()

	return d
}

func (m *Manager) initCarfileMap() {
	carfileHashs, err := cache.GetDB().GetCacheingCarfiles()
	if err != nil {
		log.Errorf("initCacheMap GetCacheingCarfiles err:%s", err.Error())
		return
	}

	for _, hash := range carfileHashs {
		cr, err := loadCarfileRecord(hash, m)
		if err != nil {
			log.Errorf("initCacheMap loadCarfileRecord hash:%s , err:%s", hash, err.Error())
			continue
		}

		m.addCarfileRecord(cr)

		// start timout check
		cr.CacheTaskMap.Range(func(key, value interface{}) bool {
			if value == nil {
				return true
			}
			c := value.(*CacheTask)
			if c == nil || c.status != api.CacheStatusRunning {
				return true
			}

			go c.startTimeoutTimer()

			return true
		})
	}
}

func (m *Manager) cacheTaskTicker() {
	ticker := time.NewTicker(time.Duration(startTaskInterval) * time.Second)
	defer ticker.Stop()

	for {
		<-ticker.C
		m.startCarfileCacheTask()
	}
}

func (m *Manager) checkExpiredTicker() {
	ticker := time.NewTicker(time.Duration(checkExpiredTimerInterval) * time.Second)
	defer ticker.Stop()

	for {
		<-ticker.C
		m.checkCachesExpired()
	}
}

func (m *Manager) resetBaseInfo() {
	count, err := persistent.GetDB().GetSuccessCachesCount()
	if err != nil {
		log.Errorf("resetBaseInfo GetSuccessCachesCount err:%s", err.Error())
		return
	}

	err = cache.GetDB().UpdateBaseInfo(cache.CarFileCountField, count)
	if err != nil {
		log.Errorf("resetBaseInfo UpdateBaseInfo err:%s", err.Error())
	}
}

// GetCarfileRecord get a carfileRecord from map or db
func (m *Manager) GetCarfileRecord(hash string) (*CarfileRecord, error) {
	dI, ok := m.CarfileRecordMap.Load(hash)
	if ok && dI != nil {
		return dI.(*CarfileRecord), nil
	}

	return loadCarfileRecord(hash, m)
}

func (m *Manager) doCarfileCacheTasks(info *api.CarfileRecordInfo) error {
	hash := info.CarfileHash
	cid := info.CarfileCid
	needReliability := info.NeedReliability
	expiredTime := info.ExpiredTime

	carfileRecord, err := loadCarfileRecord(hash, m)
	if err != nil {
		if !persistent.GetDB().IsNilErr(err) {
			return err
		}
		// not found
		carfileRecord = newCarfileRecord(m, cid, hash)
		carfileRecord.needReliability = needReliability
		carfileRecord.expiredTime = expiredTime
	} else {
		if needReliability <= carfileRecord.reliability {
			return xerrors.Errorf("reliable enough :%d/%d ", carfileRecord.reliability, needReliability)
		}
		carfileRecord.needReliability = needReliability
		carfileRecord.expiredTime = expiredTime
	}

	err = persistent.GetDB().SavceCarfileRecordInfo(&api.CarfileRecordInfo{
		CarfileCid:      carfileRecord.carfileCid,
		NeedReliability: carfileRecord.needReliability,
		ExpiredTime:     carfileRecord.expiredTime,
		CarfileHash:     carfileRecord.carfileHash,
	})
	if err != nil {
		return xerrors.Errorf("cid:%s,SavceCarfileRecordInfo err:%s", carfileRecord.carfileCid, err.Error())
	}

	isRunning := carfileRecord.restartUndoneCache()
	if !isRunning {
		isRunning, err = carfileRecord.dispatchCache()
		if err != nil {
			return err
		}
	}

	if isRunning {
		m.addCarfileRecord(carfileRecord)
	}

	return nil
}

// CacheCarfile new carfile task
func (m *Manager) CacheCarfile(cid string, reliability int, expiredTime time.Time) error {
	// if runningTaskMaxCount <= m.runningTaskCount {
	// 	return xerrors.Errorf("The cache task has exceeded %d, please wait", runningTaskMaxCount)
	// }

	hash, err := helper.CIDString2HashString(cid)
	if err != nil {
		return xerrors.Errorf("%s cid to hash err:", cid, err.Error())
	}

	info := &api.CarfileRecordInfo{CarfileHash: hash, CarfileCid: cid, NeedReliability: reliability, ExpiredTime: expiredTime}
	// return m.doCarfileTask(info)

	return cache.GetDB().PushCarfileToWaitList(info)
}

// CacheContinue continue a cache
func (m *Manager) CacheContinue(carfileCid, deviceID string) error {
	// hash, err := helper.CIDString2HashString(carfileCid)
	// if err != nil {
	// 	return err
	// }

	// dI, exist := m.CarfileRecordMap.Load(hash)
	// if exist && dI != nil {
	// 	return xerrors.Errorf("carfileRecord %s is running, please wait", carfileCid)
	// }

	return xerrors.New("unrealized")
}

// RemoveCarfileRecord remove a carfile
func (m *Manager) RemoveCarfileRecord(carfileCid string) error {
	hash, err := helper.CIDString2HashString(carfileCid)
	if err != nil {
		return err
	}

	dI, exist := m.CarfileRecordMap.Load(hash)
	if exist && dI != nil {
		return xerrors.Errorf("carfileRecord %s is running, please wait", carfileCid)
	}

	cInfos, err := persistent.GetDB().GetCachesWithHash(hash, false)
	if err != nil {
		return xerrors.Errorf("GetCachesWithHash: %s,err:%s", carfileCid, err.Error())
	}

	err = persistent.GetDB().RemoveCarfileRecord(hash)
	if err != nil {
		return xerrors.Errorf("RemoveCarfileRecord err:%s ", err.Error())
	}

	count := int64(0)
	for _, cInfo := range cInfos {
		go m.notifyNodeRemoveCarfile(cInfo.DeviceID, carfileCid)

		if cInfo.Status == api.CacheStatusSuccess {
			count++
		}
	}

	// update record to redis
	return cache.GetDB().IncrByBaseInfo(cache.CarFileCountField, -count)
}

// RemoveCache remove a cache
func (m *Manager) RemoveCache(carfileCid, deviceID string) error {
	hash, err := helper.CIDString2HashString(carfileCid)
	if err != nil {
		return err
	}

	cInfo, err := persistent.GetDB().GetCacheInfo(hash, deviceID)
	if err != nil {
		return xerrors.Errorf("GetCacheInfo: %s,err:%s", carfileCid, err.Error())
	}

	return m.removeCacheTask(cInfo, carfileCid)
}

// CacheCarfileResult block cache result
func (m *Manager) CacheCarfileResult(deviceID string, info *api.CacheResultInfo) (err error) {
	var carfileRecord *CarfileRecord
	dI, exist := m.CarfileRecordMap.Load(info.CarfileHash)
	if exist && dI != nil {
		carfileRecord = dI.(*CarfileRecord)
	} else {
		err = xerrors.Errorf("task not running : %s,%s ,err:%v", deviceID, info.CarfileHash, err)
		return
	}

	cacheI, exist := carfileRecord.CacheTaskMap.Load(deviceID)
	if !exist {
		err = xerrors.Errorf("cacheCarfileResult not found deviceID:%s,cid:%s", deviceID, carfileRecord.carfileCid)
		return
	}
	c := cacheI.(*CacheTask)

	err = c.carfileCacheResult(info)
	return
}

func (m *Manager) startCarfileCacheTask() {
	doLen := runningCarfileMaxCount - m.runningTaskCount
	if doLen <= 0 {
		return
	}

	// TODO if doLen too big,
	// lpop : Not executed after removal
	for i := 0; i < doLen; i++ {
		info, err := cache.GetDB().GetWaitCarfile()
		if err != nil {
			if cache.GetDB().IsNilErr(err) {
				return
			}
			log.Errorf("GetWaitCarfile err:%s", err.Error())
			continue
		}

		if cI, exist := m.CarfileRecordMap.Load(info.CarfileHash); exist {
			carfileRecord := cI.(*CarfileRecord)
			carfileRecord.needReliability = info.NeedReliability
			carfileRecord.expiredTime = info.ExpiredTime
			continue
		}

		err = m.doCarfileCacheTasks(info)
		if err != nil {
			log.Errorf("doCarfileCacheTasks %s err:%s", info.CarfileCid, err.Error())
		}
	}
}

func (m *Manager) addCarfileRecord(cr *CarfileRecord) {
	if cr == nil {
		log.Error("addCarfileRecord err carfileRecord is nil")
		return
	}

	_, exist := m.CarfileRecordMap.LoadOrStore(cr.carfileHash, cr)
	if !exist {
		m.runningTaskCount++
	}
}

func (m *Manager) removeCarfileRecord(cr *CarfileRecord) {
	_, exist := m.CarfileRecordMap.LoadAndDelete(cr.carfileHash)
	if exist {
		m.runningTaskCount--
	}

	m.resetLatelyExpiredTime(cr.expiredTime)
}

// StopCacheTask stop cache task
func (m *Manager) StopCacheTask(cid, deviceID string) error {
	return xerrors.New("unrealized")
}

// ReplenishCacheExpiredTime replenish time
func (m *Manager) ReplenishCacheExpiredTime(cid, deviceID string, hour int) error {
	hash, err := helper.CIDString2HashString(cid)
	if err != nil {
		return err
	}

	dI, ok := m.CarfileRecordMap.Load(hash)
	if ok && dI != nil {
		carfileRecord := dI.(*CarfileRecord)
		carfileRecord.expiredTime = carfileRecord.expiredTime.Add((time.Duration(hour) * time.Hour))
	}

	return persistent.GetDB().ExtendExpiredTimeWhitCarfile(hash, hour)
}

// ResetCacheExpiredTime reset expired time
func (m *Manager) ResetCacheExpiredTime(cid, deviceID string, expiredTime time.Time) error {
	hash, err := helper.CIDString2HashString(cid)
	if err != nil {
		return err
	}

	dI, ok := m.CarfileRecordMap.Load(hash)
	if ok && dI != nil {
		carfileRecord := dI.(*CarfileRecord)
		carfileRecord.expiredTime = expiredTime
	}

	err = persistent.GetDB().ChangeExpiredTimeWhitCarfile(hash, expiredTime)
	if err != nil {
		return err
	}

	m.resetLatelyExpiredTime(expiredTime)

	return nil
}

// NodesQuit clean node caches info and restore caches
func (m *Manager) NodesQuit(deviceIDs []string) {
	// TODO need refactor
	carfileMap, err := persistent.GetDB().UpdateCacheInfoOfQuitNode(deviceIDs)
	if err != nil {
		log.Errorf("CleanNodeAndRestoreCaches err:%s", err.Error())
		return
	}

	values := make(map[string]interface{}, 0)
	for _, deviceID := range deviceIDs {
		// update node block count
		values[deviceID] = 0

		// TODO notify node remove all carfile
		node := m.nodeManager.GetNode(deviceID)
		if node != nil {
			node.BlockCount = 0
		}
	}

	// err = cache.GetDB().UpdateDeviceInfos(cache.BlockCountField, values)
	// if err != nil {
	// 	log.Errorf("CleanNodeAndRestoreCaches UpdateDeviceInfos err:%s ", err.Error())
	// }

	m.resetBaseInfo()

	// recache
	for _, info := range carfileMap {
		// Restore cache
		err = cache.GetDB().PushCarfileToWaitList(&api.CarfileRecordInfo{CarfileHash: info.CarfileHash, CarfileCid: info.CarfileCid, NeedReliability: info.NeedReliability, ExpiredTime: info.ExpiredTime})
		if err != nil {
			log.Errorf("cleanNodeAndRestoreCaches PushCarfileToWaitList err:%s", err.Error())
			continue
		}
		log.Infof("Restore carfile :%s", info.CarfileHash)
	}
}

// check expired caches
func (m *Manager) checkCachesExpired() {
	if m.latelyExpiredTime.After(time.Now()) {
		return
	}

	//(On) : n = cache count
	carfileRecords, err := persistent.GetDB().GetExpiredCarfiles()
	if err != nil {
		log.Errorf("GetExpiredCarfiles err:%s", err.Error())
		return
	}

	for _, carfileRecord := range carfileRecords {
		// do remove
		err = m.RemoveCarfileRecord(carfileRecord.CarfileCid)
		log.Warnf("cid:%s, expired,remove it ; %v", carfileRecord.CarfileCid, err)
	}

	// reset latelyExpiredTime
	m.latelyExpiredTime, err = persistent.GetDB().GetMinExpiredTimeWithCaches()
	if err != nil {
		log.Errorf("GetMinExpiredTimeWithCaches err:%s", err.Error())
		return
	}
}

func (m *Manager) resetLatelyExpiredTime(t time.Time) {
	if m.latelyExpiredTime.After(t) {
		m.latelyExpiredTime = t
	}
}

func (m *Manager) removeCacheTask(cacheInfo *api.CacheTaskInfo, carfileCid string) error {
	dI, exist := m.CarfileRecordMap.Load(cacheInfo.CarfileHash)
	if exist && dI != nil {
		return xerrors.Errorf("task %s is running, please wait", carfileCid)
	}

	// delete cache and update carfile info
	err := persistent.GetDB().RemoveCacheTask(cacheInfo.DeviceID, cacheInfo.CarfileHash)
	if err != nil {
		return err
	}

	if cacheInfo.Status == api.CacheStatusSuccess {
		err := cache.GetDB().IncrByBaseInfo(cache.CarFileCountField, -1)
		if err != nil {
			log.Errorf("removeCache IncrByBaseInfo err:%s", err.Error())
		}
	}

	dI, ok := m.CarfileRecordMap.Load(cacheInfo.CarfileHash)
	if ok && dI != nil {
		dI.(*CarfileRecord).CacheTaskMap.Delete(cacheInfo.DeviceID)
	}

	go m.notifyNodeRemoveCarfile(cacheInfo.DeviceID, carfileCid)
	return nil
}

// Notify node to delete all carfile
func (m *Manager) notifyNodeRemoveAllCarfile(deviceID string) error {
	edge := m.nodeManager.GetEdgeNode(deviceID)
	if edge != nil {
		return edge.GetAPI().DeleteAllCarfiles(context.Background())
	}

	candidate := m.nodeManager.GetCandidateNode(deviceID)
	if candidate != nil {
		return candidate.GetAPI().DeleteAllCarfiles(context.Background())
	}

	return nil
}

// Notify node to delete carfile
func (m *Manager) notifyNodeRemoveCarfile(deviceID, cid string) error {
	edge := m.nodeManager.GetEdgeNode(deviceID)
	if edge != nil {
		return edge.GetAPI().DeleteCarfile(context.Background(), cid)
	}

	candidate := m.nodeManager.GetCandidateNode(deviceID)
	if candidate != nil {
		return candidate.GetAPI().DeleteCarfile(context.Background(), cid)
	}

	return nil
}

// Calculate the number of rootcache according to the reliability
func (m *Manager) getNeedRootCacheCount(reliability int) int {
	// TODO interim strategy
	count := (reliability / 6) + 1
	if count > 3 {
		count = 3
	}

	return count
}

// find the edges
func (m *Manager) findAppropriateEdges(cacheMap sync.Map, count int) []*node.Node {
	list := make([]*node.Node, 0)
	if count <= 0 {
		return list
	}

	m.nodeManager.EdgeNodeMap.Range(func(key, value interface{}) bool {
		deviceID := key.(string)

		if _, exist := cacheMap.Load(deviceID); exist {
			return true
		}

		node := value.(*node.EdgeNode)
		if node.DiskUsage > diskUsageMax {
			return true
		}

		list = append(list, node.Node)
		return true
	})

	sort.Slice(list, func(i, j int) bool {
		return list[i].GetCurCacheCount() < list[j].GetCurCacheCount()
	})

	if count > len(list) {
		count = len(list)
	}

	return list[0:count]
}

// find the candidates
func (m *Manager) findAppropriateCandidates(cacheMap sync.Map, count int) []*node.Node {
	list := make([]*node.Node, 0)
	if count <= 0 {
		return list
	}

	m.nodeManager.CandidateNodeMap.Range(func(key, value interface{}) bool {
		deviceID := key.(string)

		if _, exist := cacheMap.Load(deviceID); exist {
			return true
		}

		node := value.(*node.CandidateNode)
		if node.DiskUsage > diskUsageMax {
			return true
		}

		list = append(list, node.Node)
		return true
	})

	sort.Slice(list, func(i, j int) bool {
		return list[i].GetCurCacheCount() < list[j].GetCurCacheCount()
	})

	if count > len(list) {
		count = len(list)
	}

	return list[0:count]
}
