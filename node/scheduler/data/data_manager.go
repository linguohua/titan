package data

import (
	"fmt"
	"sync"
	"time"

	logging "github.com/ipfs/go-log/v2"
	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/node/helper"
	"github.com/linguohua/titan/node/scheduler/db/cache"
	"github.com/linguohua/titan/node/scheduler/db/persistent"
	"github.com/linguohua/titan/node/scheduler/errmsg"
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
)

// Manager Data
type Manager struct {
	nodeManager        *node.Manager
	blockLoaderCh      chan bool
	dataTaskLoaderCh   chan bool
	taskMap            sync.Map
	timerInterval      int //  time interval (Second)
	runningTaskMax     int
	expiredTimeOfCache time.Time
	defaultTime        time.Time

	haveCacheNodes map[string]time.Time
}

// NewDataManager new
func NewDataManager(nodeManager *node.Manager) *Manager {
	d := &Manager{
		nodeManager:      nodeManager,
		blockLoaderCh:    make(chan bool, 1),
		dataTaskLoaderCh: make(chan bool, 1),
		timerInterval:    10,
		runningTaskMax:   5,
	}

	go d.run()

	return d
}

func (m *Manager) run() {
	ticker := time.NewTicker(time.Duration(m.timerInterval) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			m.checkTaskTimeouts()
			m.notifyDataLoader()
			m.checkCachesExpired()
		case <-m.dataTaskLoaderCh:
			m.doDataTasks()
		case <-m.blockLoaderCh:
			m.doCacheResults()
		}
	}
}

func (m *Manager) getWaitingDataTask(index int64) (api.DataInfo, error) {
	info, err := cache.GetDB().GetWaitingDataTask(index)
	if err != nil {
		return info, err
	}

	// Find the next task if the task is in progress
	if m.isDataTaskRunnning(info.CarfileHash, "") {
		return m.getWaitingDataTask(index + 1)
	}

	return info, nil
}

func (m *Manager) doDataTask() error {
	info, err := m.getWaitingDataTask(0)
	if err != nil {
		if cache.GetDB().IsNilErr(err) {
			return nil
		}
		return xerrors.Errorf("getWaitingTask err:%s", err.Error())
	}

	var c *Cache
	defer func() {
		if err != nil {
			cacheID := ""
			if c != nil {
				cacheID = c.CacheID
			}
			m.saveEvent(info.CarfileCid, cacheID, "", err.Error(), eventTypeDoDataTaskErr)
		} else {
			m.dataTaskStart(c.Data)
		}

		err = cache.GetDB().RemoveWaitingDataTask(info)
		if err != nil {
			log.Errorf("RemoveWaitingDataTask err:%s", err.Error())
		}
	}()

	if info.CacheInfos != nil && len(info.CacheInfos) > 0 {
		cacheID := info.CacheInfos[0].CacheID

		c, err = m.makeDataContinue(info.CarfileHash, cacheID)
		if err != nil {
			return xerrors.Errorf("makeDataContinue err:%s", err.Error())
		}
	} else {
		c, err = m.makeDataTask(info.CarfileCid, info.CarfileHash, info.NeedReliability, info.ExpiredTime)
		if err != nil {
			return xerrors.Errorf("makeDataTask err:%s", err.Error())
		}
	}

	return nil
}

// GetData get a data from map or db
func (m *Manager) GetData(hash string) *Data {
	dI, ok := m.taskMap.Load(hash)
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

	cI, ok := data.CacheMap.Load(taskInfo.CacheID)
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

func (m *Manager) makeDataTask(cid, hash string, reliability int, expiredTime time.Time) (*Cache, error) {
	var err error
	data := m.GetData(hash)
	if data == nil {
		data = newData(m.nodeManager, m, cid, hash, reliability)
		data.ExpiredTime = expiredTime
	} else {
		if reliability <= data.Reliability {
			return nil, xerrors.Errorf("reliable enough :%d/%d ", data.Reliability, reliability)
		}
		data.NeedReliability = reliability
		data.ExpiredTime = expiredTime
	}

	// log.Warnf("askCacheData reliability:%d,data.needReliability:%d,data.reliability:%d", reliability, data.needReliability, data.reliability)

	err = persistent.GetDB().SetDataInfo(&api.DataInfo{
		CarfileCid:      data.CarfileCid,
		TotalSize:       data.TotalSize,
		NeedReliability: data.NeedReliability,
		Reliability:     data.Reliability,
		CacheCount:      data.CacheCount,
		TotalBlocks:     data.TotalBlocks,
		ExpiredTime:     data.ExpiredTime,
		CarfileHash:     data.CarfileHash,
	})
	if err != nil {
		return nil, xerrors.Errorf("cid:%s,SetDataInfo err:%s", data.CarfileCid, err.Error())
	}

	data.CacheCount = data.Reliability

	return data.dispatchCache(data.getOldUndoneCache())
}

func (m *Manager) makeDataContinue(hash, cacheID string) (*Cache, error) {
	data := m.GetData(hash)
	if data == nil {
		return nil, xerrors.Errorf("%s,cid:%s,cacheID:%s", errmsg.ErrNotFoundTask, hash, cacheID)
	}

	cacheI, ok := data.CacheMap.Load(cacheID)
	if !ok || cacheI == nil {
		return nil, xerrors.Errorf("Not Found CacheID :%s", cacheID)
	}
	cache := cacheI.(*Cache)

	if cache.Status == api.CacheStatusSuccess {
		return nil, xerrors.Errorf("Cache completed :%s", cacheID)
	}

	data.CacheCount = data.Reliability

	return data.dispatchCache(cache)
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
		return xerrors.Errorf("%s : %s", errmsg.ErrNotFoundTask, carfileCid)
	}

	data.CacheMap.Range(func(key, value interface{}) bool {
		c := value.(*Cache)

		err := c.removeCache()
		if err != nil {
			log.Errorf("cacheID:%s, removeBlocks err:%s", c.CacheID, err.Error())
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
		return xerrors.Errorf("%s : %s", errmsg.ErrNotFoundTask, carfileCid)
	}

	cacheI, ok := data.CacheMap.Load(cacheID)
	if !ok {
		return xerrors.Errorf("removeCache not found cacheID:%s,Cid:%s", cacheID, data.CarfileCid)
	}
	cache := cacheI.(*Cache)

	err = cache.removeCache()
	e := ""
	if err != nil {
		e = err.Error()
		log.Errorf("cacheID:%s, removeCache err:%s", cache.CacheID, err.Error())
	}

	return m.saveEvent(carfileCid, cacheID, "user", e, eventTypeRemoveCacheEnd)
}

func (m *Manager) cacheCarfileResult(deviceID string, info *api.CacheResultInfo) error {
	// area := m.nodeManager.getNodeArea(deviceID)
	data := m.GetData(info.CarFileHash)
	if data == nil {
		return xerrors.Errorf("%s : %s", errmsg.ErrNotFoundTask, info.CarFileHash)
	}

	if !m.isDataTaskRunnning(info.CarFileHash, info.CacheID) {
		return xerrors.Errorf("data not running : %s,%s", info.CacheID, info.Cid)
	}

	cacheI, ok := data.CacheMap.Load(info.CacheID)
	if !ok {
		return xerrors.Errorf("cacheCarfileResult not found cacheID:%s,Cid:%s", info.CacheID, data.CarfileCid)
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
	case m.blockLoaderCh <- true:
	default:
	}
}

func (m *Manager) doDataTasks() {
	doLen := m.runningTaskMax - len(m.GetRunningTasks())
	if doLen > 0 {
		for i := 0; i < doLen; i++ {
			err := m.doDataTask()
			if err != nil {
				log.Errorf("doDataTask err:%s", err.Error())
			}
		}
	}
}

func (m *Manager) notifyDataLoader() {
	select {
	case m.dataTaskLoaderCh <- true:
	default:
	}
}

// update the data task timeout
func (m *Manager) updateDataTimeout(carfileHash, cacheID string, timeout int64) {
	err := cache.GetDB().SetRunningDataTask(carfileHash, cacheID, timeout)
	if err != nil {
		log.Panicf("dataTaskStart %s , SetRunningDataTask err:%s", cacheID, err.Error())
	}
}

func (m *Manager) dataTaskStart(data *Data) {
	if data == nil {
		log.Error("dataTaskStart err data is nil")
		return
	}
	m.saveEvent(data.CarfileCid, "", "", "", eventTypeDoDataTaskStart)

	m.taskMap.Store(data.CarfileHash, data)
}

func (m *Manager) dataTaskEnd(cid, hash, msg string) {
	m.saveEvent(cid, "", "", msg, eventTypeDoDataTaskEnd)

	m.taskMap.Delete(hash)

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
		return xerrors.New(errmsg.ErrCidNotFind)
	}

	cI, ok := data.CacheMap.Load(cID)
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

	for range nodes {
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
		return xerrors.Errorf("%s:%s", errmsg.ErrCidNotFind, hash)
	}

	m.saveEvent(cid, cacheID, "", fmt.Sprintf("add hour:%d", hour), eventTypeReplenishCacheTime)

	if cacheID != "" {
		cI, ok := data.CacheMap.Load(cacheID)
		if ok && cI != nil {
			cache := cI.(*Cache)
			cache.replenishExpiredTime(hour)
		}
	} else {
		data.CacheMap.Range(func(key, value interface{}) bool {
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
		return xerrors.Errorf("%s:%s", errmsg.ErrCidNotFind, hash)
	}

	m.saveEvent(cid, cacheID, "", fmt.Sprintf("expiredTime:%s", expiredTime.String()), eventTypeResetCacheTime)

	if cacheID != "" {
		cI, ok := data.CacheMap.Load(cacheID)
		if ok && cI != nil {
			cache := cI.(*Cache)
			cache.resetExpiredTime(expiredTime)
		}
	} else {
		data.CacheMap.Range(func(key, value interface{}) bool {
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

		// err = m.CacheData(info.CarfileCid, info.NeedReliability, info.ExpiredTime)
		// if err != nil {
		// 	log.Errorf("cleanNodeAndRestoreCaches err:%s", err.Error())
		// 	continue
		// }

	}

	// update node block count
	err = cache.GetDB().UpdateDeviceInfo(deviceID, func(deviceInfo *api.DevicesInfo) {
		deviceInfo.BlockCount = 0
	})
	if err != nil {
		log.Errorf("UpdateDeviceInfo err:%s ", err.Error())
	}
}

// check expired caches
func (m *Manager) checkCachesExpired() {
	if m.expiredTimeOfCache.Equal(m.defaultTime) {
		var err error
		m.expiredTimeOfCache, err = persistent.GetDB().GetMinExpiredTimeWithCaches()
		if err != nil {
			// log.Errorf("GetMinExpiredTimeWithCaches err:%s", err.Error())
			return
		}
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

		cI, ok := data.CacheMap.Load(cacheInfo.CacheID)
		if !ok {
			continue
		}
		cache := cI.(*Cache)

		// do remove
		err := cache.removeCache()
		if err != nil {
			m.saveEvent(data.CarfileCid, cache.CacheID, "expired", err.Error(), eventTypeRemoveCacheEnd)
		} else {
			m.saveEvent(data.CarfileCid, cache.CacheID, "expired", "", eventTypeRemoveCacheEnd)
		}
	}

	m.expiredTimeOfCache = m.defaultTime
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
