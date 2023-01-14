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
	checkCacheTimeoutInterval = 65      //  set to check node cache timeout timer(Unit:Second)
	nodeCacheTimeout          = 70      //  expiration set to redis  (Unit:Second)
	startTaskInterval         = 10      //  time interval (Unit:Second)
	checkExpiredTimerInterval = 60 * 30 //  time interval (Unit:Second)

	// It needs to be changed to the number of caches
	runningCarfileMaxCount = 10

	// If the node disk size is greater than this value, caching will not continue
	diskUsageMax = 90.0
)

// Manager carfile
type Manager struct {
	nodeManager        *node.Manager
	CarfileRecordMap   sync.Map // cacheing carfile map
	expiredTimeOfCache time.Time
	isLoadExpiredTime  bool
	getAuthToken       func() []byte
	runningTaskCount   int
}

// NewCarfileManager new
func NewCarfileManager(nodeManager *node.Manager, getToken func() []byte) *Manager {
	d := &Manager{
		nodeManager:       nodeManager,
		isLoadExpiredTime: true,
		getAuthToken:      getToken,
	}

	d.initCacheMap()
	d.resetBaseInfo()
	go d.cacheTaskTicker()
	go d.checkExpiredTicker()

	return d
}

func (m *Manager) initCacheMap() {
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
			if c == nil {
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
		m.doCarfileTasks()
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
	infos, err := persistent.GetDB().GetSuccessCaches()
	if err != nil {
		log.Errorf("resetBaseInfo GetSuccessCaches err:%s", err.Error())
		return
	}

	err = cache.GetDB().UpdateBaseInfo(cache.CarFileCountField, len(infos))
	if err != nil {
		log.Errorf("resetBaseInfo UpdateBaseInfo err:%s", err.Error())
	}
}

// func (m *Manager) getWaitingCarfileTasks(count int) []*api.CarfileRecordInfo {
// 	list := make([]*api.CarfileRecordInfo, 0)

// 	curCount := int64(0)
// 	isLoad := true

// 	for isLoad {
// 		info, err := cache.GetDB().GetWaitCarfile(curCount)
// 		if err != nil {
// 			if cache.GetDB().IsNilErr(err) {
// 				isLoad = false
// 				continue
// 			}
// 			log.Errorf("getWaitingCarfileTasks err:%s", err.Error())
// 			continue
// 		}

// 		curCount++

// 		if _, exist := m.CarfileRecordMap.Load(info.CarfileHash); exist {
// 			continue
// 		}

// 		list = append(list, info)
// 		if len(list) >= count {
// 			isLoad = false
// 		}
// 	}

// 	return list
// }

// func (m *Manager) doCarfileTask(info *api.CarfileRecordInfo) error {
// 	// if info.CacheInfos != nil && len(info.CacheInfos) > 0 {
// 	// 	deviceID := info.CacheInfos[0].DeviceID

// 	// 	err := m.makeDataContinue(info.CarfileHash, deviceID)
// 	// 	if err != nil {
// 	// 		return err
// 	// 	}
// 	// } else {
// 	err := m.makeCarfileTask(info)
// 	if err != nil {
// 		return err
// 	}
// 	// }

// 	return nil
// }

// GetCarfileRecord get a carfileRecord from map or db
func (m *Manager) GetCarfileRecord(hash string) (*CarfileRecord, error) {
	dI, ok := m.CarfileRecordMap.Load(hash)
	if ok && dI != nil {
		return dI.(*CarfileRecord), nil
	}

	return loadCarfileRecord(hash, m)
}

func (m *Manager) doCarfileTask(info *api.CarfileRecordInfo) error {
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

	err = persistent.GetDB().CreateCarfileInfo(&api.CarfileRecordInfo{
		CarfileCid:      carfileRecord.carfileCid,
		NeedReliability: carfileRecord.needReliability,
		ExpiredTime:     carfileRecord.expiredTime,
		CarfileHash:     carfileRecord.carfileHash,
	})
	if err != nil {
		return xerrors.Errorf("cid:%s,CreateCarfileInfo err:%s", carfileRecord.carfileCid, err.Error())
	}

	isRunning, err := carfileRecord.dispatchCache()
	if err != nil {
		return err
	}

	if isRunning {
		m.addCarfileRecord(carfileRecord)
	}

	return nil
}

func (m *Manager) makeDataContinue(hash, deviceID string) error {
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
func (m *Manager) CacheContinue(cid, deviceID string) error {
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

	successList := make([]*api.CacheTaskInfo, 0)
	for _, cInfo := range cInfos {
		go m.notifyNodeRemoveCarfile(cInfo.DeviceID, carfileCid)

		if cInfo.Status == api.CacheStatusSuccess {
			successList = append(successList, cInfo)
		}
	}

	// update record to redis
	return cache.GetDB().RemoveCarfileRecord(successList)
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
		err = xerrors.Errorf("cacheCarfileResult not found cacheID:%s,Cid:%s", deviceID, carfileRecord.carfileCid)
		return
	}
	c := cacheI.(*CacheTask)

	err = c.carfileCacheResult(info)
	return
}

func (m *Manager) doCarfileTasks() {
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

		err = m.doCarfileTask(info)
		if err != nil {
			log.Errorf("doCarfileTask %s err:%s", info.CarfileCid, err.Error())
		}
	}
	// list := m.getWaitingCarfileTasks(doLen)
	// if len(list) <= 0 {
	// 	return
	// }

	// for _, info := range list {
	// 	err := m.doCarfileTask(info)
	// 	if err != nil {
	// 		log.Errorf("doCacheTasks err:%s", err.Error())
	// 	}
	// }

	// err := cache.GetDB().RemoveWaitCarfiles(list)
	// if err != nil {
	// 	log.Errorf("doDataTasks RemoveWaitCarfiles err:%s", err.Error())
	// }
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

func (m *Manager) removeCarfileRecord(hash string) {
	_, exist := m.CarfileRecordMap.LoadAndDelete(hash)
	if exist {
		m.runningTaskCount--
	}
}

// StopCacheTask stop cache data
func (m *Manager) StopCacheTask(cid, deviceID string) error {
	return xerrors.New("unrealized")
}

// ReplenishExpiredTimeToData replenish time
func (m *Manager) ReplenishExpiredTimeToData(cid, cacheID string, hour int) error {
	hash, err := helper.CIDString2HashString(cid)
	if err != nil {
		return err
	}

	dI, ok := m.CarfileRecordMap.Load(hash)
	if ok && dI != nil {
		data := dI.(*CarfileRecord)

		if cacheID != "" {
			cI, ok := data.CacheTaskMap.Load(cacheID)
			if ok && cI != nil {
				c := cI.(*CacheTask)
				c.expiredTime = c.expiredTime.Add((time.Duration(hour) * time.Hour))
			}

			return xerrors.Errorf("not found cache :%s", cacheID)
		}

		data.CacheTaskMap.Range(func(key, value interface{}) bool {
			if value != nil {
				c := value.(*CacheTask)
				if c != nil {
					c.expiredTime = c.expiredTime.Add((time.Duration(hour) * time.Hour))
				}
			}

			return true
		})
	}

	return persistent.GetDB().ExtendExpiredTimeWhitCaches(hash, cacheID, hour)
}

// ResetExpiredTime reset expired time
func (m *Manager) ResetExpiredTime(cid, cacheID string, expiredTime time.Time) error {
	hash, err := helper.CIDString2HashString(cid)
	if err != nil {
		return err
	}

	dI, ok := m.CarfileRecordMap.Load(hash)
	if ok && dI != nil {
		data := dI.(*CarfileRecord)

		if cacheID != "" {
			cI, ok := data.CacheTaskMap.Load(cacheID)
			if ok && cI != nil {
				c := cI.(*CacheTask)
				c.expiredTime = expiredTime
			}

			return xerrors.Errorf("not found cache :%s", cacheID)
		}

		data.CacheTaskMap.Range(func(key, value interface{}) bool {
			if value != nil {
				c := value.(*CacheTask)
				if c != nil {
					c.expiredTime = expiredTime
				}
			}

			return true
		})
	}

	return persistent.GetDB().ChangeExpiredTimeWhitCaches(hash, cacheID, expiredTime)
}

// CleanNodeAndRestoreCaches clean a node caches info and restore caches
func (m *Manager) CleanNodeAndRestoreCaches(deviceIDs []string) {
	// TODO avoid loops
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
	}

	err = cache.GetDB().UpdateDeviceInfos(cache.BlockCountField, values)
	if err != nil {
		log.Errorf("CleanNodeAndRestoreCaches UpdateDeviceInfos err:%s ", err.Error())
	}

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
	// TODO Check the expiration policy to be adjusted
	if m.isLoadExpiredTime {
		var err error
		//(On) : n = cache count
		m.expiredTimeOfCache, err = persistent.GetDB().GetMinExpiredTimeWithCaches()
		if err != nil {
			// log.Errorf("GetMinExpiredTimeWithCaches err:%s", err.Error())
			return
		}

		m.isLoadExpiredTime = false
	}

	if m.expiredTimeOfCache.After(time.Now()) {
		return
	}

	//(On) : n = cache count
	cacheInfos, err := persistent.GetDB().GetExpiredCaches()
	if err != nil {
		log.Errorf("GetExpiredCaches err:%s", err.Error())
		return
	}

	for _, cacheInfo := range cacheInfos {
		carfileCid, err := helper.HashString2CidString(cacheInfo.CarfileHash)
		if err != nil {
			continue
		}

		// do remove
		err = m.removeCacheTask(cacheInfo, carfileCid)
		log.Warnf("hash:%s,device:%s expired,remove it ; %v", carfileCid, cacheInfo.DeviceID, err)
	}

	m.isLoadExpiredTime = true
}

func (m *Manager) removeCacheTask(cacheInfo *api.CacheTaskInfo, carfileCid string) error {
	dI, exist := m.CarfileRecordMap.Load(cacheInfo.CarfileHash)
	if exist && dI != nil {
		return xerrors.Errorf("task %s is running, please wait", carfileCid)
	}

	// delete cache and update data info
	err := persistent.GetDB().RemoveCacheAndUpdateData(cacheInfo.DeviceID, cacheInfo.CarfileHash)
	if err != nil {
		return err
	}

	if cacheInfo.Status == api.CacheStatusSuccess {
		err := cache.GetDB().RemoveCacheTask(cacheInfo.DeviceID, cacheInfo.DoneSize, cacheInfo.DoneBlocks)
		if err != nil {
			log.Errorf("removeCache RemoveCacheTask err:%s", err.Error())
		}
	}

	dI, ok := m.CarfileRecordMap.Load(cacheInfo.CarfileHash)
	if ok && dI != nil {
		dI.(*CarfileRecord).CacheTaskMap.Delete(cacheInfo.DeviceID)
	}

	go m.notifyNodeRemoveCarfile(cacheInfo.DeviceID, carfileCid)
	return nil
}

// Notify nodes to delete blocks
func (m *Manager) notifyNodeRemoveCarfile(deviceID, cid string) error {
	edge := m.nodeManager.GetEdgeNode(deviceID)
	if edge != nil {
		_, err := edge.GetAPI().DeleteCarfile(context.Background(), cid)
		return err
	}

	candidate := m.nodeManager.GetCandidateNode(deviceID)
	if candidate != nil {
		_, err := candidate.GetAPI().DeleteCarfile(context.Background(), cid)
		return err
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
