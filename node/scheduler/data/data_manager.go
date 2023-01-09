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

var nodeCacheResultInterval = 60 //(Second)

// EventType event
type EventType string

const (
	eventTypeDoCacheTaskStart    EventType = "Start_Cache"
	eventTypeDoCacheTaskEnd      EventType = "End_Cache"
	eventTypeDoDataTaskErr       EventType = "Data_Error"
	eventTypeDoDataTaskStart     EventType = "Start_Data"
	eventTypeDoDataTaskEnd       EventType = "End_Data"
	eventTypeAddNewDataTask      EventType = "Add_New_Data"
	eventTypeAddContinueDataTask EventType = "Add_Continue_Data"
	eventTypeRemoveData          EventType = "Remove_Data"
	eventTypeRemoveCache         EventType = "Remove_Cache"
	eventTypeStopDataTask        EventType = "Stop_Data"
	eventTypeReplenishCacheTime  EventType = "Replenish_Cache_Expired"
	eventTypeResetCacheTime      EventType = "Reset_Cache_Expired"
	eventTypeRestoreCache        EventType = "Restore_Cache"

	dataCacheTimerInterval    = 10     //  time interval (Second)
	checkExpiredTimerInterval = 60 * 5 //  time interval (Second)

	runningTaskMaxCount = 5
	// blockResultThreadCount = 10
)

// Manager Data
type Manager struct {
	nodeManager        *node.Manager
	dataMap            sync.Map
	expiredTimeOfCache time.Time
	isLoadExpiredTime  bool
}

// NewDataManager new
func NewDataManager(nodeManager *node.Manager) *Manager {
	d := &Manager{
		nodeManager:       nodeManager,
		isLoadExpiredTime: true,
	}

	d.resetBaseInfo()
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
			m.doDataTasks()
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

		isRunning, err := m.isDataTaskRunnning(info.CarfileHash, "")
		if err != nil || isRunning {
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
		deviceID := info.CacheInfos[0].DeviceID

		err := m.makeDataContinue(info.CarfileHash, deviceID)
		if err != nil {
			return err
		}
	} else {
		err := m.makeDataTask(info.CarfileCid, info.CarfileHash, info.NeedReliability, info.ExpiredTime)
		if err != nil {
			return err
		}
	}

	return nil
}

// GetData get a data from map or db
func (m *Manager) GetData(hash string) (*Data, error) {
	dI, ok := m.dataMap.Load(hash)
	if ok && dI != nil {
		return dI.(*Data), nil
	}

	return loadData(hash, m)
}

func (m *Manager) checkTaskTimeout(taskInfo *cache.DataTask) {
	isRunning, err := m.isDataTaskRunnning(taskInfo.CarfileHash, taskInfo.DeviceID)
	if err != nil || isRunning {
		return
	}

	data, err := m.GetData(taskInfo.CarfileHash)
	if data == nil {
		return
	}

	cI, ok := data.CacheMap.Load(taskInfo.DeviceID)
	if ok && cI != nil {
		cache := cI.(*Cache)
		err := cache.endCache(api.CacheStatusTimeout)
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
	data, err := m.GetData(hash)
	if persistent.GetDB().IsNilErr(err) {
		data = newData(m, cid, hash)
		data.needReliability = reliability
		data.expiredTime = expiredTime
	} else {
		if reliability <= data.reliability {
			return xerrors.Errorf("reliable enough :%d/%d ", data.reliability, reliability)
		}
		data.needReliability = reliability
		data.expiredTime = expiredTime
	}

	err = persistent.GetDB().SetDataInfo(&api.DataInfo{
		CarfileCid:      data.carfileCid,
		TotalSize:       data.totalSize,
		NeedReliability: data.needReliability,
		Reliability:     data.reliability,
		TotalBlocks:     data.totalBlocks,
		ExpiredTime:     data.expiredTime,
		CarfileHash:     data.carfileHash,
	})
	if err != nil {
		return xerrors.Errorf("cid:%s,SetDataInfo err:%s", data.carfileCid, err.Error())
	}

	needCandidate := m.getNeedRootCacheCount(data.needReliability) - data.rootCaches
	data.candidates = m.findAppropriateCandidates(needCandidate)
	data.edges = m.findAppropriateEdges(data.needReliability - data.reliability)

	errList := data.dispatchCache()
	if len(errList) > 0 {
		for deviceID, e := range errList {
			log.Errorf("cache deviceID:%s, err:%s", deviceID, e)
			//TODO add node to waiting list
		}
	}

	m.recordTaskStart(data)
	return nil
}

func (m *Manager) makeDataContinue(hash, deviceID string) error {
	// data, err := m.GetData(hash)
	// if err != nil {
	// 	return xerrors.Errorf("not found data task,cid:%s,cacheID:%s,err:%s", hash, deviceID, err.Error())
	// }

	// cacheI, ok := data.CacheMap.Load(deviceID)
	// if !ok || cacheI == nil {
	// 	return xerrors.Errorf("not found cacheID :%s", deviceID)
	// }
	// cache := cacheI.(*Cache)

	// if cache.status == api.CacheStatusSuccess {
	// 	return xerrors.Errorf("cache completed :%s", deviceID)
	// }

	// err := data.dispatchCache()
	// if err != nil {
	// 	return err
	// }

	// m.recordTaskStart(data)
	return nil
}

// CacheData new data task
func (m *Manager) CacheData(cid string, reliability int, expiredTime time.Time) error {
	hash, err := helper.CIDString2HashString(cid)
	if err != nil {
		return xerrors.Errorf("%s cid to hash err:", cid, err.Error())
	}

	err = cache.GetDB().SetWaitingDataTask(&api.DataInfo{CarfileHash: hash, CarfileCid: cid, NeedReliability: reliability, ExpiredTime: expiredTime})
	if err != nil {
		return err
	}

	err = saveEvent(cid, "", "user", fmt.Sprintf("reliability:%d", reliability), eventTypeAddNewDataTask)
	if err != nil {
		return err
	}

	// m.notifyDataLoader()

	return nil
}

// CacheContinue continue a cache
func (m *Manager) CacheContinue(cid, deviceID string) error {
	hash, err := helper.CIDString2HashString(cid)
	if err != nil {
		return xerrors.Errorf("%s cid to hash err:", cid, err.Error())
	}

	err = cache.GetDB().SetWaitingDataTask(&api.DataInfo{CarfileHash: hash, CarfileCid: cid, CacheInfos: []api.CacheInfo{{DeviceID: deviceID}}})
	if err != nil {
		return err
	}

	err = saveEvent(cid, deviceID, "user", "", eventTypeAddContinueDataTask)
	if err != nil {
		return err
	}

	// m.notifyDataLoader()

	return nil
}

// RemoveCarfile remove a carfile
func (m *Manager) RemoveCarfile(carfileCid string) error {
	hash, err := helper.CIDString2HashString(carfileCid)
	if err != nil {
		return err
	}

	isRunning, err := m.isDataTaskRunnning(hash, "")
	if err != nil || isRunning {
		return xerrors.Errorf("data is running , please try again later")
	}

	data, err := m.GetData(hash)
	if err != nil {
		return xerrors.Errorf("not found data task: %s,err:%s", carfileCid, err.Error())
	}

	err = saveEvent(carfileCid, "", "user", "", eventTypeRemoveData)
	if err != nil {
		return err
	}

	data.CacheMap.Range(func(key, value interface{}) bool {
		c := value.(*Cache)

		err := c.removeCache()
		if err != nil {
			log.Errorf("cacheID:%s, removeBlocks err:%s", c.deviceID, err.Error())
		}

		return true
	})

	return nil
}

// RemoveCache remove a cache
func (m *Manager) RemoveCache(carfileCid, cacheID string) error {
	hash, err := helper.CIDString2HashString(carfileCid)
	if err != nil {
		return err
	}

	isRunning, err := m.isDataTaskRunnning(hash, "")
	if err != nil || isRunning {
		return xerrors.Errorf("data is running , please try again later")
	}

	data, err := m.GetData(hash)
	if err != nil {
		return xerrors.Errorf("not found data task: %s,err:%s", carfileCid, err.Error())
	}

	cacheI, exist := data.CacheMap.Load(cacheID)
	if !exist {
		return xerrors.Errorf("removeCache not found cacheID:%s,Cid:%s", cacheID, data.carfileCid)
	}
	cache := cacheI.(*Cache)

	err = cache.removeCache()
	e := ""
	if err != nil {
		e = err.Error()
	}

	return saveEvent(carfileCid, cacheID, "user", e, eventTypeRemoveCache)
}

// CacheCarfileResult block cache result
func (m *Manager) CacheCarfileResult(deviceID string, info *api.CacheResultInfo) (err error) {
	var data *Data
	dI, exist := m.dataMap.Load(info.CarfileHash)
	if exist && dI != nil {
		data = dI.(*Data)
	} else {
		data, _ = loadData(info.CarfileHash, m)
		if data == nil {
			return xerrors.Errorf("not found data task: %s", info.CarfileHash)
		}

		m.dataMap.Store(info.CarfileHash, data)
		defer func() {
			if err != nil {
				m.dataMap.Delete(info.CarfileHash)
			}
		}()
	}

	isRunning, err := m.isDataTaskRunnning(info.CarfileHash, deviceID)
	if err != nil || !isRunning {
		err = xerrors.Errorf("data not running : %s,%s ,err:%v", deviceID, info.CarfileHash, err)
		return
	}

	cacheI, exist := data.CacheMap.Load(deviceID)
	if !exist {
		err = xerrors.Errorf("cacheCarfileResult not found cacheID:%s,Cid:%s", deviceID, data.carfileCid)
		return
	}
	c := cacheI.(*Cache)

	err = c.blockCacheResult(info)
	return
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
			err = saveEvent(info.CarfileCid, "", "", err.Error(), eventTypeDoDataTaskErr)
			if err != nil {
				log.Errorf("doDataTasks saveEvent err:%s", err.Error())
			}
		}
	}

	err := cache.GetDB().RemoveWaitingDataTasks(list)
	if err != nil {
		log.Errorf("doDataTasks RemoveWaitingDataTasks err:%s", err.Error())
	}
}

// func (m *Manager) notifyDataLoader() {
// 	select {
// 	case m.dataTaskLoaderCh <- true:
// 	default:
// 	}
// }

// update the data task timeout
func (m *Manager) updateDataTimeout(carfileHash, deviceID string, timeoutSecond int64, addSecond int64) {
	et, err := cache.GetDB().GetRunningDataTaskExpiredTime(carfileHash, deviceID)
	if err != nil {
		log.Errorf("updateDataTimeout GetRunningDataTaskExpiredTime err:%s", err.Error())
		return
	}

	t := int64(et.Seconds())
	if t > timeoutSecond {
		if addSecond <= 0 {
			return
		}

		timeoutSecond = t
	}

	timeoutSecond += addSecond

	err = cache.GetDB().SetRunningDataTask(carfileHash, deviceID, timeoutSecond)
	if err != nil {
		log.Panicf("dataTaskStart %s , SetRunningDataTask err:%s", deviceID, err.Error())
	}
}

func (m *Manager) recordTaskStart(data *Data) {
	if data == nil {
		log.Error("recordTaskStart err data is nil")
		return
	}

	err := saveEvent(data.carfileCid, "", "", "", eventTypeDoDataTaskStart)
	if err != nil {
		log.Errorf("recordTaskStart saveEvent err:%s", err.Error())
	}

	m.dataMap.Store(data.carfileHash, data)
}

func (m *Manager) recordTaskEnd(cid, hash, msg string) {
	err := saveEvent(cid, "", "", msg, eventTypeDoDataTaskEnd)
	if err != nil {
		log.Errorf("recordTaskEnd saveEvent err:%s", err.Error())
	}

	m.dataMap.Delete(hash)

	// continue task
	// m.notifyDataLoader()
}

// GetRunningTasks get running tasks
func (m *Manager) GetRunningTasks() []*cache.DataTask {
	list, err := cache.GetDB().GetDataTasksWithRunningList()
	if err != nil {
		log.Errorf("GetDataTasksWithRunningList err:%s", err.Error())
		return make([]*cache.DataTask, 0)
	}

	return list
}

// StopCacheTask stop cache data
func (m *Manager) StopCacheTask(cid string) error {
	hash, err := helper.CIDString2HashString(cid)
	if err != nil {
		return err
	}

	//TODO
	deviceID := ""
	// deviceID, err := cache.GetDB().GetRunningDataTask(hash)
	// if err != nil {
	// 	return err
	// }

	data, err := m.GetData(hash)
	if err != nil {
		return xerrors.Errorf("not found cid:%s,err:%s", cid, err.Error())
	}

	err = saveEvent(cid, deviceID, "", "", eventTypeStopDataTask)
	if err != nil {
		return err
	}

	cI, ok := data.CacheMap.Load(deviceID)
	if ok && cI != nil {
		cache := cI.(*Cache)
		err := cache.endCache(api.CacheStatusFail)
		if err != nil {
			return err
		}
	} else {
		err := cache.GetDB().RemoveRunningDataTask(hash, deviceID)
		if err != nil {
			return xerrors.Errorf("endCache RemoveRunningDataTask err: %s", err.Error())
		}
	}

	go m.removeWaitCacheBlockWithNode(deviceID, cid)

	return nil
}

func (m *Manager) removeWaitCacheBlockWithNode(deviceID, cid string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cNode := m.nodeManager.GetCandidateNode(deviceID)
	if cNode != nil {
		err := cNode.GetAPI().RemoveWaitCacheBlockWith(ctx, cid)
		if err != nil {
			log.Errorf("%s , RemoveWaitCacheBlockWith err:%s", deviceID, err.Error())
		}
		return err
	}

	eNode := m.nodeManager.GetEdgeNode(deviceID)
	if eNode != nil {
		err := eNode.GetAPI().RemoveWaitCacheBlockWith(ctx, cid)
		if err != nil {
			log.Errorf("%s , RemoveWaitCacheBlockWith err:%s", deviceID, err.Error())
		}

		return err
	}

	return nil
}

func (m *Manager) isDataTaskRunnning(hash, deviceID string) (bool, error) {
	cID, err := cache.GetDB().GetRunningDataTask(hash, deviceID)
	if err != nil && !cache.GetDB().IsNilErr(err) {
		log.Errorf("isTaskRunnning %s GetRunningDataTask err:%s", hash, err.Error())
		return false, err
	}

	if deviceID == "" {
		return cID != "", nil
	}

	return cID == deviceID, nil
}

// ReplenishExpiredTimeToData replenish time
func (m *Manager) ReplenishExpiredTimeToData(cid, cacheID string, hour int) error {
	hash, err := helper.CIDString2HashString(cid)
	if err != nil {
		return err
	}

	err = saveEvent(cid, cacheID, "", fmt.Sprintf("add hour:%d", hour), eventTypeReplenishCacheTime)
	if err != nil {
		return err
	}

	dI, ok := m.dataMap.Load(hash)
	if ok && dI != nil {
		data := dI.(*Data)

		if cacheID != "" {
			cI, ok := data.CacheMap.Load(cacheID)
			if ok && cI != nil {
				c := cI.(*Cache)
				c.expiredTime = c.expiredTime.Add((time.Duration(hour) * time.Hour))
			}

			return xerrors.Errorf("not found cache :%s", cacheID)
		}

		data.CacheMap.Range(func(key, value interface{}) bool {
			if value != nil {
				c := value.(*Cache)
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

	err = saveEvent(cid, cacheID, "", fmt.Sprintf("expiredTime:%s", expiredTime.String()), eventTypeResetCacheTime)
	if err != nil {
		return err
	}

	dI, ok := m.dataMap.Load(hash)
	if ok && dI != nil {
		data := dI.(*Data)

		if cacheID != "" {
			cI, ok := data.CacheMap.Load(cacheID)
			if ok && cI != nil {
				c := cI.(*Cache)
				c.expiredTime = expiredTime
			}

			return xerrors.Errorf("not found cache :%s", cacheID)
		}

		data.CacheMap.Range(func(key, value interface{}) bool {
			if value != nil {
				c := value.(*Cache)
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
	count := 0
	recacheMap := make(map[string]string)
	for _, deviceID := range deviceIDs {
		cacheCount, carfileMap, err := persistent.GetDB().UpdateCacheInfoOfQuitNode(deviceID)
		if err != nil {
			if !persistent.GetDB().IsNilErr(err) {
				log.Errorf("%s UpdateCacheInfoOfQuitNode err:%s", deviceID, err.Error())
			}
			continue
		}

		count += cacheCount

		for cid := range carfileMap {
			recacheMap[cid] = deviceID
		}

		// update node block count
		err = cache.GetDB().UpdateDeviceInfo(deviceID, cache.BlockCountField, 0)
		if err != nil {
			log.Errorf("CleanNodeAndRestoreCaches UpdateDeviceInfo err:%s ", err.Error())
		}
	}

	err := cache.GetDB().IncrByBaseInfo(cache.CarFileCountField, int64(-count))
	if err != nil {
		log.Errorf("CleanNodeAndRestoreCaches IncrByBaseInfo err:%s", err.Error())
	}

	// log.Warnf("recacheMap : %v", recacheMap)
	// recache
	for carfileHash, deviceID := range recacheMap {
		info, err := persistent.GetDB().GetDataInfo(carfileHash)
		if err != nil {
			log.Errorf("cleanNodeAndRestoreCaches GetDataInfo err:%s", err.Error())
			continue
		}

		// Restore cache
		err = cache.GetDB().SetWaitingDataTask(&api.DataInfo{CarfileHash: carfileHash, CarfileCid: info.CarfileCid, NeedReliability: info.NeedReliability, ExpiredTime: info.ExpiredTime})
		if err != nil {
			log.Errorf("cleanNodeAndRestoreCaches SetWaitingDataTask err:%s", err.Error())
			continue
		}

		err = persistent.GetDB().SetEventInfo(&api.EventInfo{CID: info.CarfileCid, DeviceID: deviceID, Msg: fmt.Sprintf("%s quitted", deviceID), Event: string(eventTypeRestoreCache)})
		if err != nil {
			log.Errorf("cleanNodeAndRestoreCaches SetEventInfo err:%s", err.Error())
			continue
		}
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
		data, err := m.GetData(cacheInfo.CarfileHash)
		if err != nil {
			continue
		}

		cI, exist := data.CacheMap.Load(cacheInfo.DeviceID)
		if !exist {
			continue
		}
		cache := cI.(*Cache)

		// do remove
		err = cache.removeCache()
		if err != nil {
			err = saveEvent(data.carfileCid, cacheInfo.DeviceID, "expired", err.Error(), eventTypeRemoveCache)
		} else {
			err = saveEvent(data.carfileCid, cacheInfo.DeviceID, "expired", "", eventTypeRemoveCache)
		}

		if err != nil {
			log.Errorf("checkCachesExpired saveEvent err:%s", err.Error())
		}
	}

	m.isLoadExpiredTime = true
}

//Calculate the number of rootcache according to the reliability
func (m *Manager) getNeedRootCacheCount(reliability int) int {
	// TODO interim strategy
	count := reliability % 5
	if count > 3 {
		count = 3
	}

	return count
}

// find the edges
func (m *Manager) findAppropriateEdges(count int) []string {
	//TODO
	return nil
}

// find the candidates
func (m *Manager) findAppropriateCandidates(count int) []string {
	//TODO
	return nil
}

func saveEvent(cid, cacheID, userID, msg string, event EventType) error {
	return persistent.GetDB().SetEventInfo(&api.EventInfo{CID: cid, User: userID, Msg: msg, Event: string(event), CacheID: cacheID})
}
