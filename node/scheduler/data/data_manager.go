package data

import (
	"context"
	"fmt"
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

var log = logging.Logger("data")

// EventType event
type EventType string

const (
	eventTypeDoCacheTaskStart    EventType = "DoCacheTaskStart"
	eventTypeDoCacheTaskEnd      EventType = "DoCacheTaskEnd"
	eventTypeDoDataTaskErr       EventType = "DoDataTaskErr"
	eventTypeDoDataTaskStart     EventType = "DoDataTaskStart"
	eventTypeDoDataTaskEnd       EventType = "DoDataTaskEnd"
	eventTypeAddNewDataTask      EventType = "AddNewDataTask"
	eventTypeAddContinueDataTask EventType = "AddContinueDataTask"
	eventTypeRemoveDataStart     EventType = "RemoveDataStart"
	eventTypeRemoveCacheStart    EventType = "RemoveCacheStart"
	eventTypeRemoveDataEnd       EventType = "RemoveDataEnd"
	eventTypeRemoveCacheEnd      EventType = "RemoveCacheEnd"
	eventTypeStopDataTask        EventType = "StopDataTask"
	eventTypeReplenishCacheTime  EventType = "ReplenishCacheTime"
	eventTypeResetCacheTime      EventType = "ResetCacheTime"
	eventTypeRestoreCache        EventType = "RestoreCache"

	dataCacheTimerInterval    = 10     //  time interval (Second)
	checkExpiredTimerInterval = 60 * 5 //  time interval (Second)

	runningTaskMaxCount = 5
)

// Manager Data
type Manager struct {
	nodeManager         *node.Manager
	blockResultLoaderCh chan bool
	// dataTaskLoaderCh    chan bool
	dataMap            sync.Map
	expiredTimeOfCache time.Time
	isLoadExpiredTime  bool

	haveCacheNodes map[string]time.Time
}

// NewDataManager new
func NewDataManager(nodeManager *node.Manager) *Manager {
	d := &Manager{
		nodeManager:         nodeManager,
		blockResultLoaderCh: make(chan bool, 1),
		// dataTaskLoaderCh:    make(chan bool, 1),
		isLoadExpiredTime: true,
	}

	d.initBaseInfo()
	go d.dataCacheTicker()
	go d.checkExpiredTicker()

	return d
}

func (m *Manager) dataCacheTicker() {
	ticker := time.NewTicker(time.Duration(dataCacheTimerInterval) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			m.checkTaskTimeouts()
			// m.notifyDataLoader()
			// case <-m.dataTaskLoaderCh:
			m.doDataTasks()
		case <-m.blockResultLoaderCh:
			m.doCacheResults()
		}
	}
}

func (m *Manager) checkExpiredTicker() {
	ticker := time.NewTicker(time.Duration(checkExpiredTimerInterval) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			m.checkCachesExpired()
		}
	}
}

func (m *Manager) initBaseInfo() {
	infos, err := persistent.GetDB().GetSuccessCaches()
	if err != nil {
		log.Warnf("initSystemData GetSuccessCaches err:%s", err.Error())
		return
	}

	err = cache.GetDB().UpdateBaseInfo("CarfileCount", len(infos))
}

func (m *Manager) getWaitingDataTasks(count int) []*api.DataInfo {
	list := make([]*api.DataInfo, 0)

	curCount := int64(0)
	isGet := true

	for isGet {
		info, err := cache.GetDB().GetWaitingDataTask(curCount)
		if err != nil {
			if cache.GetDB().IsNilErr(err) {
				isGet = false
				continue
			}
			log.Errorf("GetWaitingDataTask err:%s", err.Error())
			continue
		}

		curCount++

		if m.isDataTaskRunnning(info.CarfileHash, "") {
			continue
		}

		list = append(list, info)
		if len(list) >= count {
			isGet = false
		}
	}

	return list
}

func (m *Manager) doDataTask(info *api.DataInfo) error {
	if info.CacheInfos != nil && len(info.CacheInfos) > 0 {
		cacheID := info.CacheInfos[0].CacheID

		err := m.makeDataContinue(info.CarfileHash, cacheID)
		if err != nil {
			return xerrors.Errorf("makeDataContinue err:%s", err.Error())
		}
	} else {
		err := m.makeDataTask(info.CarfileCid, info.CarfileHash, info.NeedReliability, info.ExpiredTime)
		if err != nil {
			return xerrors.Errorf("makeDataTask err:%s", err.Error())
		}
	}

	return nil
}

// GetData get a data from map or db
func (m *Manager) GetData(hash string) *Data {
	dI, ok := m.dataMap.Load(hash)
	if ok && dI != nil {
		return dI.(*Data)
	}

	data := loadData(hash, m.nodeManager, m)
	if data != nil {
		return data
	}

	return nil
}

func (m *Manager) checkTaskTimeout(taskInfo cache.DataTask) {
	if m.isDataTaskRunnning(taskInfo.CarfileHash, taskInfo.CacheID) {
		return
	}

	data := m.GetData(taskInfo.CarfileHash)
	if data == nil {
		return
	}

	cI, ok := data.cacheMap.Load(taskInfo.CacheID)
	if ok && cI != nil {
		cache := cI.(*Cache)
		err := cache.endCache(0, api.CacheStatusTimeout)
		if err != nil {
			log.Errorf("stopCache err:%s", err.Error())
		}
	}
}

func (m *Manager) checkTaskTimeouts() {
	list := m.GetRunningTasks()
	if len(list) <= 0 {
		return
	}

	for _, taskInfo := range list {
		m.checkTaskTimeout(taskInfo)
	}
}

func (m *Manager) makeDataTask(cid, hash string, reliability int, expiredTime time.Time) error {
	var err error
	data := m.GetData(hash)
	if data == nil {
		data = newData(m.nodeManager, m, cid, hash, reliability)
		data.expiredTime = expiredTime
	} else {
		if reliability <= data.reliability {
			return xerrors.Errorf("reliable enough :%d/%d ", data.reliability, reliability)
		}
		data.needReliability = reliability
		data.expiredTime = expiredTime
	}

	// log.Warnf("askCacheData reliability:%d,data.needReliability:%d,data.reliability:%d", reliability, data.needReliability, data.reliability)

	err = persistent.GetDB().SetDataInfo(&api.DataInfo{
		CarfileCid:      data.carfileCid,
		TotalSize:       data.totalSize,
		NeedReliability: data.needReliability,
		Reliability:     data.reliability,
		CacheCount:      data.cacheCount,
		TotalBlocks:     data.totalBlocks,
		ExpiredTime:     data.expiredTime,
		CarfileHash:     data.carfileHash,
	})
	if err != nil {
		return xerrors.Errorf("cid:%s,SetDataInfo err:%s", data.carfileCid, err.Error())
	}

	data.cacheCount = data.reliability

	err = data.dispatchCache(data.getUndoneCache())
	if err != nil {
		return err
	}

	m.recordTaskStart(data)
	return nil
}

func (m *Manager) makeDataContinue(hash, cacheID string) error {
	data := m.GetData(hash)
	if data == nil {
		return xerrors.Errorf("Not Found Data Task,cid:%s,cacheID:%s", hash, cacheID)
	}

	cacheI, ok := data.cacheMap.Load(cacheID)
	if !ok || cacheI == nil {
		return xerrors.Errorf("Not Found CacheID :%s", cacheID)
	}
	cache := cacheI.(*Cache)

	if cache.status == api.CacheStatusSuccess {
		return xerrors.Errorf("Cache completed :%s", cacheID)
	}

	data.cacheCount = data.reliability

	err := data.dispatchCache(cache)
	if err != nil {
		return err
	}

	m.recordTaskStart(data)
	return nil
}

// CacheData new data task
func (m *Manager) CacheData(cid string, reliability int, expiredTime time.Time) error {
	hash, err := helper.CIDString2HashString(cid)
	if err != nil {
		return xerrors.Errorf("%s cid to hash err:", cid, err.Error())
	}

	// TODO check reliability expiredTime

	err = cache.GetDB().SetWaitingDataTask(api.DataInfo{CarfileHash: hash, CarfileCid: cid, NeedReliability: reliability, ExpiredTime: expiredTime})
	if err != nil {
		return err
	}

	err = m.saveEvent(cid, "", "user", fmt.Sprintf("reliability:%d", reliability), eventTypeAddNewDataTask)
	if err != nil {
		return err
	}

	// m.notifyDataLoader()

	return nil
}

// CacheContinue continue a cache
func (m *Manager) CacheContinue(cid, cacheID string) error {
	hash, err := helper.CIDString2HashString(cid)
	if err != nil {
		return xerrors.Errorf("%s cid to hash err:", cid, err.Error())
	}

	err = cache.GetDB().SetWaitingDataTask(api.DataInfo{CarfileHash: hash, CarfileCid: cid, CacheInfos: []api.CacheInfo{{CacheID: cacheID}}})
	if err != nil {
		return err
	}

	err = m.saveEvent(cid, cacheID, "user", "", eventTypeAddContinueDataTask)
	if err != nil {
		return err
	}

	// m.notifyDataLoader()

	return nil
}

// RemoveCarfile remove a carfile
func (m *Manager) RemoveCarfile(carfileCid string) error {
	err := m.saveEvent(carfileCid, "", "user", "", eventTypeRemoveDataStart)
	if err != nil {
		return err
	}

	hash, err := helper.CIDString2HashString(carfileCid)
	if err != nil {
		return err
	}

	data := m.GetData(hash)
	if data == nil {
		return xerrors.Errorf("Not Found Data Task: %s", carfileCid)
	}

	data.cacheMap.Range(func(key, value interface{}) bool {
		c := value.(*Cache)

		err := c.removeCache()
		if err != nil {
			log.Errorf("cacheID:%s, removeBlocks err:%s", c.cacheID, err.Error())
		}

		return true
	})

	return m.saveEvent(carfileCid, "", "user", "", eventTypeRemoveDataEnd)
}

// RemoveCache remove a cache
func (m *Manager) RemoveCache(carfileCid, cacheID string) error {
	err := m.saveEvent(carfileCid, cacheID, "user", "", eventTypeRemoveCacheStart)
	if err != nil {
		return err
	}

	hash, err := helper.CIDString2HashString(carfileCid)
	if err != nil {
		return err
	}

	data := m.GetData(hash)
	if data == nil {
		return xerrors.Errorf("Not Found Data Task: %s", carfileCid)
	}

	cacheI, ok := data.cacheMap.Load(cacheID)
	if !ok {
		return xerrors.Errorf("removeCache not found cacheID:%s,Cid:%s", cacheID, data.carfileCid)
	}
	cache := cacheI.(*Cache)

	err = cache.removeCache()
	e := ""
	if err != nil {
		e = err.Error()
		log.Errorf("cacheID:%s, removeCache err:%s", cache.cacheID, err.Error())
	}

	return m.saveEvent(carfileCid, cacheID, "user", e, eventTypeRemoveCacheEnd)
}

func (m *Manager) cacheCarfileResult(deviceID string, info *api.CacheResultInfo) error {
	data := m.GetData(info.CarFileHash)
	if data == nil {
		return xerrors.Errorf("Not Found Data Task: %s", info.CarFileHash)
	}

	if !m.isDataTaskRunnning(info.CarFileHash, info.CacheID) {
		return xerrors.Errorf("data not running : %s,%s", info.CacheID, info.Cid)
	}

	cacheI, ok := data.cacheMap.Load(info.CacheID)
	if !ok {
		return xerrors.Errorf("cacheCarfileResult not found cacheID:%s,Cid:%s", info.CacheID, data.carfileCid)
	}
	c := cacheI.(*Cache)

	return c.blockCacheResult(info)
}

func (m *Manager) doCacheResults() {
	// defer m.notifyBlockLoader()
	for cache.GetDB().GetCacheResultNum() > 0 {
		info, err := cache.GetDB().GetCacheResultInfo()
		if err != nil {
			log.Errorf("doResultTask GetCacheResultInfo err:%s", err.Error())
			return
		}

		err = m.cacheCarfileResult(info.DeviceID, &info)
		if err != nil {
			log.Errorf("doResultTask cacheCarfileResult err:%s", err.Error())
			// return
		}

		// err = cache.GetDB().RemoveCacheResultInfo()
		// if err != nil {
		// 	log.Errorf("doResultTask RemoveCacheResultInfo err:%s", err.Error())
		// 	return
		// }
	}
}

// PushCacheResultToQueue new cache task
func (m *Manager) PushCacheResultToQueue(deviceID string, info *api.CacheResultInfo) error {
	info.DeviceID = deviceID

	err := cache.GetDB().SetCacheResultInfo(*info)

	m.notifyBlockLoader()

	return err
}

func (m *Manager) notifyBlockLoader() {
	select {
	case m.blockResultLoaderCh <- true:
	default:
	}
}

func (m *Manager) doDataTasks() {
	doLen := runningTaskMaxCount - len(m.GetRunningTasks())
	if doLen <= 0 {
		return
	}

	list := m.getWaitingDataTasks(doLen)
	if len(list) <= 0 {
		return
	}

	for _, info := range list {
		err := m.doDataTask(info)
		if err != nil {
			// log.Errorf("doDataTask err:%s", err.Error())
			m.saveEvent(info.CarfileCid, "", "", err.Error(), eventTypeDoDataTaskErr)
		}
	}

	err := cache.GetDB().RemoveWaitingDataTasks(list)
	if err != nil {
		log.Errorf("RemoveWaitingDataTasks err:%s", err.Error())
	}
}

// func (m *Manager) notifyDataLoader() {
// 	select {
// 	case m.dataTaskLoaderCh <- true:
// 	default:
// 	}
// }

// update the data task timeout
func (m *Manager) updateDataTimeout(carfileHash, cacheID string, timeout int64) {
	et, err := cache.GetDB().GetRunningDataTaskExpiredTime(carfileHash)
	if err == nil {
		if int64(et.Seconds()) > timeout {
			return
		}
	}

	err = cache.GetDB().SetRunningDataTask(carfileHash, cacheID, timeout)
	if err != nil {
		log.Panicf("dataTaskStart %s , SetRunningDataTask err:%s", cacheID, err.Error())
	}
}

func (m *Manager) recordTaskStart(data *Data) {
	if data == nil {
		log.Error("recordTaskStart err data is nil")
		return
	}
	m.saveEvent(data.carfileCid, "", "", "", eventTypeDoDataTaskStart)

	m.dataMap.Store(data.carfileHash, data)
}

func (m *Manager) recordTaskEnd(cid, hash, msg string) {
	m.saveEvent(cid, "", "", msg, eventTypeDoDataTaskEnd)

	m.dataMap.Delete(hash)

	// continue task
	// m.notifyDataLoader()
}

// GetRunningTasks get running tasks
func (m *Manager) GetRunningTasks() []cache.DataTask {
	list, err := cache.GetDB().GetDataTasksWithRunningList()
	if err != nil {
		log.Errorf("GetDataTasksWithRunningList err:%s", err.Error())
		return make([]cache.DataTask, 0)
	}

	return list
}

// StopCacheTask stop cache data
func (m *Manager) StopCacheTask(cid string) error {
	hash, err := helper.CIDString2HashString(cid)
	if err != nil {
		return err
	}

	cID, err := cache.GetDB().GetRunningDataTask(hash)
	if err != nil && !cache.GetDB().IsNilErr(err) {
		return err
	}

	data := m.GetData(hash)
	if data == nil {
		return xerrors.Errorf("Not Found Cid:%s", cid)
	}

	cI, ok := data.cacheMap.Load(cID)
	if ok && cI != nil {
		cache := cI.(*Cache)
		err := cache.endCache(0, api.CacheStatusFail)
		if err != nil {
			return err
		}
	}

	m.saveEvent(cid, cID, "", "", eventTypeStopDataTask)

	nodes, err := persistent.GetDB().GetNodesFromCache(cID)
	if err != nil {
		return err
	}

	for _, deviceID := range nodes {
		cNode := m.nodeManager.GetCandidateNode(deviceID)
		if cNode != nil {
			err = cNode.GetAPI().RemoveWaitCacheBlockWith(context.Background(), cid)
			if err != nil {
				log.Errorf("%s , RemoveWaitCacheBlockWith err:%s", deviceID, err.Error())
				continue
			}
		}

		eNode := m.nodeManager.GetEdgeNode(deviceID)
		if eNode != nil {
			err = eNode.GetAPI().RemoveWaitCacheBlockWith(context.Background(), cid)
			if err != nil {
				log.Errorf("%s , RemoveWaitCacheBlockWith err:%s", deviceID, err.Error())
				continue
			}
		}
	}

	return nil
}

func (m *Manager) isDataTaskRunnning(hash, cacheID string) bool {
	cID, err := cache.GetDB().GetRunningDataTask(hash)
	if err != nil && !cache.GetDB().IsNilErr(err) {
		log.Errorf("isTaskRunnning %s GetRunningDataTask err:%s", hash, err.Error())
		return true
	}

	if cacheID == "" {
		return cID != ""
	}

	return cID == cacheID
}

func (m *Manager) saveEvent(cid, cacheID, userID, msg string, event EventType) error {
	return persistent.GetDB().SetEventInfo(&api.EventInfo{CID: cid, User: userID, Msg: msg, Event: string(event), CacheID: cacheID})
}

// ReplenishExpiredTimeToData replenish time
func (m *Manager) ReplenishExpiredTimeToData(cid, cacheID string, hour int) error {
	hash, err := helper.CIDString2HashString(cid)
	if err != nil {
		return err
	}

	data := m.GetData(hash)
	if data == nil {
		return xerrors.Errorf("Not Found Cid:%s", cid)
	}

	m.saveEvent(cid, cacheID, "", fmt.Sprintf("add hour:%d", hour), eventTypeReplenishCacheTime)

	if cacheID != "" {
		cI, ok := data.cacheMap.Load(cacheID)
		if ok && cI != nil {
			cache := cI.(*Cache)
			cache.replenishExpiredTime(hour)
		}
	} else {
		data.cacheMap.Range(func(key, value interface{}) bool {
			if value != nil {
				cache := value.(*Cache)
				if cache != nil {
					cache.replenishExpiredTime(hour)
				}
			}

			return true
		})
	}

	return nil
}

// ResetExpiredTime reset expired time
func (m *Manager) ResetExpiredTime(cid, cacheID string, expiredTime time.Time) error {
	hash, err := helper.CIDString2HashString(cid)
	if err != nil {
		return err
	}

	data := m.GetData(hash)
	if data == nil {
		return xerrors.Errorf("Not Found Cid:%s", cid)
	}

	m.saveEvent(cid, cacheID, "", fmt.Sprintf("expiredTime:%s", expiredTime.String()), eventTypeResetCacheTime)

	if cacheID != "" {
		cI, ok := data.cacheMap.Load(cacheID)
		if ok && cI != nil {
			cache := cI.(*Cache)
			cache.resetExpiredTime(expiredTime)
		}
	} else {
		data.cacheMap.Range(func(key, value interface{}) bool {
			if value != nil {
				cache := value.(*Cache)
				if cache != nil {
					cache.resetExpiredTime(expiredTime)
				}
			}

			return true
		})
	}

	return nil
}

// CleanNodeAndRestoreCaches clean a node caches info and restore caches
func (m *Manager) CleanNodeAndRestoreCaches(deviceID string) {
	// find node caches
	caches, err := persistent.GetDB().GetCachesFromNode(deviceID)
	if err != nil {
		log.Errorf("cleanNodeAndRestoreCaches GetCachesFromNode err:%s", err.Error())
		return
	}

	if len(caches) <= 0 {
		log.Warn("cleanNodeAndRestoreCaches caches is nil")
		return
	}

	for _, c := range caches {
		blocks, err := persistent.GetDB().GetCacheBlocksSizeWithNode(deviceID, c.CacheID)
		if err != nil {
			log.Errorf("GetCacheBlocksWithNode err:%s", err.Error())
			continue
		}

		c.DoneBlocks -= len(blocks)
		for _, size := range blocks {
			c.DoneSize -= size
		}

		if c.Status == api.CacheStatusSuccess {
			err = cache.GetDB().IncrByBaseInfo("CarfileCount", -1)
		}
	}

	// clean node caches and change cache info \ data info
	err = persistent.GetDB().CleanCacheDataWithNode(deviceID, caches)
	if err != nil {
		log.Errorf("cleanNodeAndRestoreCaches CleanCacheDataWithNode err:%s", err.Error())
		return
	}

	// restore cache info
	dataMap := make(map[string]int)
	for _, c := range caches {
		dataMap[c.CarfileHash]++
	}

	for carfileHash := range dataMap {
		info, err := persistent.GetDB().GetDataInfo(carfileHash)
		if err != nil {
			log.Errorf("cleanNodeAndRestoreCaches GetDataInfo err:%s", err.Error())
			continue
		}

		// Restore cache
		err = cache.GetDB().SetWaitingDataTask(api.DataInfo{CarfileHash: carfileHash, CarfileCid: info.CarfileCid, NeedReliability: info.NeedReliability, ExpiredTime: info.ExpiredTime})
		if err != nil {
			log.Errorf("cleanNodeAndRestoreCaches SetWaitingDataTask err:%s", err.Error())
			continue
		}

		err = persistent.GetDB().SetEventInfo(&api.EventInfo{CID: info.CarfileCid, DeviceID: deviceID, Msg: fmt.Sprintf("%s exited", deviceID), Event: string(eventTypeRestoreCache)})
		if err != nil {
			log.Errorf("cleanNodeAndRestoreCaches SetEventInfo err:%s", err.Error())
			continue
		}

	}

	// update node block count
	err = cache.GetDB().UpdateDeviceInfo(deviceID, "BlockCount", 0)
	if err != nil {
		log.Errorf("UpdateDeviceInfo err:%s ", err.Error())
	}
}

// check expired caches
func (m *Manager) checkCachesExpired() {
	if m.isLoadExpiredTime {
		var err error
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

	cacheInfos, err := persistent.GetDB().GetExpiredCaches()
	if err != nil {
		log.Errorf("GetExpiredCaches err:%s", err.Error())
		return
	}

	for _, cacheInfo := range cacheInfos {
		data := m.GetData(cacheInfo.CarfileHash)
		if data == nil {
			continue
		}

		cI, ok := data.cacheMap.Load(cacheInfo.CacheID)
		if !ok {
			continue
		}
		cache := cI.(*Cache)

		// do remove
		err := cache.removeCache()
		if err != nil {
			m.saveEvent(data.carfileCid, cache.cacheID, "expired", err.Error(), eventTypeRemoveCacheEnd)
		} else {
			m.saveEvent(data.carfileCid, cache.cacheID, "expired", "", eventTypeRemoveCacheEnd)
		}
	}

	m.isLoadExpiredTime = true
}

// // expired
// func (m *Manager) checkCacheExpired(hash string) error {
// 	data := m.getData(hash)
// 	if data == nil {
// 		return xerrors.Errorf("%s:%s", ErrCidNotFind, hash)
// 	}

// 	now := time.Now()
// 	data.cacheMap.Range(func(key, value interface{}) bool {
// 		c := value.(*Cache)
// 		if c.expiredTime.After(now) {
// 			return true
// 		}

// 		// do remove
// 		err := c.removeCache()
// 		if err != nil {
// 			m.saveEvent(hash, c.cacheID, "expired", err.Error(), eventTypeRemoveCacheEnd)
// 		} else {
// 			m.saveEvent(hash, c.cacheID, "expired", "", eventTypeRemoveCacheEnd)
// 		}

// 		return true
// 	})

// 	return nil
// }
