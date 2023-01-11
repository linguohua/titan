package data

import (
	"context"
	"fmt"
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

var log = logging.Logger("data")

var nodeCacheResultInterval = int64(70) //(Second)

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

	runningTaskMaxCount = 10
	// blockResultThreadCount = 10

	// If the node disk size is greater than this value, caching will not continue
	diskUsageMax = 90
)

// Manager Data
type Manager struct {
	nodeManager        *node.Manager
	CarfileRecordMap   sync.Map
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
	go d.dataCacheTicker()
	go d.checkExpiredTicker()

	return d
}

func (m *Manager) initCacheMap() {
	carfiles, err := cache.GetDB().GetCacheingCarfiles()
	if err != nil {
		log.Errorf("initCacheMap GetCacheingCarfiles err:%s", err.Error())
		return
	}

	for hash := range carfiles {
		cr, err := loadCarfileRecord(hash, m)
		if err != nil {
			log.Errorf("initCacheMap loadCarfileRecord hash:%s , err:%s", hash, err.Error())
			continue
		}

		m.CarfileRecordMap.Store(hash, cr)
		m.runningTaskCount++
	}
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

func (m *Manager) getWaitingDataTasks(count int) []*api.CarfileRecordInfo {
	list := make([]*api.CarfileRecordInfo, 0)

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

		if _, exist := m.CarfileRecordMap.Load(info.CarfileHash); exist {
			continue
		}

		list = append(list, info)
		if len(list) >= count {
			isGet = false
		}
	}

	return list
}

func (m *Manager) doDataTask(info *api.CarfileRecordInfo) error {
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

// GetCarfileRecord get a data from map or db
func (m *Manager) GetCarfileRecord(hash string) (*CarfileRecord, error) {
	dI, ok := m.CarfileRecordMap.Load(hash)
	if ok && dI != nil {
		return dI.(*CarfileRecord), nil
	}

	return loadCarfileRecord(hash, m)
}

func (m *Manager) checkTaskTimeout(taskInfo *cache.DataTask) {
	hash, err := cache.GetDB().GetNodeCacheingCarfile(taskInfo.DeviceID)
	if err != nil || hash != "" {
		return
	}

	data, err := m.GetCarfileRecord(taskInfo.CarfileHash)
	if data == nil {
		return
	}

	cI, ok := data.CacheTaskMap.Load(taskInfo.DeviceID)
	if ok && cI != nil {
		cache := cI.(*CacheTask)
		err := cache.endCache(api.CacheStatusTimeout)
		if err != nil {
			log.Errorf("stopCache err:%s", err.Error())
		}
	}
}

func (m *Manager) checkTaskTimeouts() {
	timeoutCarfiles, err := cache.GetDB().GetCacheTimeoutNodes()
	if err != nil {
		log.Errorf("checkTaskTimeouts GetCacheTimeoutNodes err:%s", err.Error())
		return
	}

	if len(timeoutCarfiles) <= 0 {
		return
	}

	for hash, devices := range timeoutCarfiles {
		dI, exist := m.CarfileRecordMap.Load(hash)
		if !exist || dI == nil {
			continue
		}
		data := dI.(*CarfileRecord)

		for _, deviceID := range devices {
			cI, exist := data.CacheTaskMap.Load(deviceID)
			if !exist || cI == nil {
				continue
			}
			cache := cI.(*CacheTask)

			err := cache.endCache(api.CacheStatusTimeout)
			if err != nil {
				log.Errorf("stopCache err:%s", err.Error())
			}
		}
	}
}

func (m *Manager) makeDataTask(cid, hash string, reliability int, expiredTime time.Time) error {
	data, err := m.GetCarfileRecord(hash)
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

	err = persistent.GetDB().SetDataInfo(&api.CarfileRecordInfo{
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
	data.candidates = m.findAppropriateCandidates(data.CacheTaskMap, needCandidate)
	data.edges = m.findAppropriateEdges(data.CacheTaskMap, data.needReliability-data.reliability)

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

	err = cache.GetDB().SetWaitingDataTask(&api.CarfileRecordInfo{CarfileHash: hash, CarfileCid: cid, NeedReliability: reliability, ExpiredTime: expiredTime})
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

	err = cache.GetDB().SetWaitingDataTask(&api.CarfileRecordInfo{CarfileHash: hash, CarfileCid: cid, CacheInfos: []api.CacheTaskInfo{{DeviceID: deviceID}}})
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

	cInfos, err := persistent.GetDB().GetCachesWithData(hash, false)
	if err != nil {
		return xerrors.Errorf("GetCachesWithData: %s,err:%s", carfileCid, err.Error())
	}

	err = persistent.GetDB().RemoveCarfileRecord(hash)
	if err != nil {
		return xerrors.Errorf("RemoveCarfileRecord err:%s ", err.Error())
	}

	dataTasks := make([]*cache.DataTask, 0)
	values := make(map[string]int64)
	carfileCount := 0
	for _, cInfo := range cInfos {
		blocks, err := m.notifyNodeRemoveCarfile(cInfo.DeviceID, carfileCid, cInfo.DoneBlocks)
		if err != nil {
			return err
		}

		if cInfo.Status == api.CacheStatusSuccess {
			carfileCount++
		}

		values[cInfo.DeviceID] = int64(-blocks)
		dataTasks = append(dataTasks, &cache.DataTask{CarfileHash: cInfo.CarfileHash, DeviceID: cInfo.DeviceID})
	}

	// update record to redis
	err = cache.GetDB().RemoveCarfileRecord(dataTasks, -int64(carfileCount), values)
	if err != nil {
		log.Errorf("RemoveCarfileRecord err:%s ", err.Error())
	}

	m.CarfileRecordMap.Delete(hash)
	m.runningTaskCount--

	return saveEvent(carfileCid, "", "user", "", eventTypeRemoveData)
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

	eMsg := ""
	err = m.removeCache(cInfo, carfileCid)
	if err != nil {
		eMsg = err.Error()
	}

	return saveEvent(carfileCid, deviceID, "user", eMsg, eventTypeRemoveCache)
}

// CacheCarfileResult block cache result
func (m *Manager) CacheCarfileResult(deviceID string, info *api.CacheResultInfo) (err error) {
	var data *CarfileRecord
	dI, exist := m.CarfileRecordMap.Load(info.CarfileHash)
	if exist && dI != nil {
		data = dI.(*CarfileRecord)
	} else {
		err = xerrors.Errorf("data not running : %s,%s ,err:%v", deviceID, info.CarfileHash, err)
		return
	}

	// isRunning, err := m.isCacheTaskRunnning(info.CarfileHash, deviceID)
	// if err != nil || !isRunning {
	// 	err = xerrors.Errorf("data not running : %s,%s ,err:%v", deviceID, info.CarfileHash, err)
	// 	return
	// }

	cacheI, exist := data.CacheTaskMap.Load(deviceID)
	if !exist {
		err = xerrors.Errorf("cacheCarfileResult not found cacheID:%s,Cid:%s", deviceID, data.carfileCid)
		return
	}
	c := cacheI.(*CacheTask)

	err = c.blockCacheResult(info)
	return
}

func (m *Manager) doDataTasks() {
	doLen := runningTaskMaxCount - m.runningTaskCount
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

func (m *Manager) recordTaskStart(data *CarfileRecord) {
	if data == nil {
		log.Error("recordTaskStart err data is nil")
		return
	}

	err := saveEvent(data.carfileCid, "", "", "", eventTypeDoDataTaskStart)
	if err != nil {
		log.Errorf("recordTaskStart saveEvent err:%s", err.Error())
	}

	m.CarfileRecordMap.Store(data.carfileHash, data)
	m.runningTaskCount++
}

func (m *Manager) recordTaskEnd(cid, hash, msg string) {
	err := saveEvent(cid, "", "", msg, eventTypeDoDataTaskEnd)
	if err != nil {
		log.Errorf("recordTaskEnd saveEvent err:%s", err.Error())
	}

	m.CarfileRecordMap.Delete(hash)
	m.runningTaskCount--

	// continue task
	// m.notifyDataLoader()
}

// GetRunningTasks get running tasks
// func (m *Manager) GetRunningTasks() map[string]int {
// 	carfiles, err := cache.GetDB().GetCacheingCarfiles()
// 	if err != nil {
// 		log.Errorf("GetCacheingNodes err:%s", err.Error())
// 		return make([]*cache.DataTask, 0)
// 	}

// 	return list
// }

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

	data, err := m.GetCarfileRecord(hash)
	if err != nil {
		return xerrors.Errorf("not found cid:%s,err:%s", cid, err.Error())
	}

	err = saveEvent(cid, deviceID, "", "", eventTypeStopDataTask)
	if err != nil {
		return err
	}

	cI, ok := data.CacheTaskMap.Load(deviceID)
	if ok && cI != nil {
		cache := cI.(*CacheTask)
		err := cache.endCache(api.CacheStatusFail)
		if err != nil {
			return err
		}
	} else {
		err := cache.GetDB().SetCacheEnd(hash, deviceID)
		if err != nil {
			return xerrors.Errorf("endCache SetCacheEnd err: %s", err.Error())
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

// func (m *Manager) isCacheTaskRunnning(hash, deviceID string) (bool, error) {
// 	cID, err := cache.GetDB().GetRunningDataTask(hash, deviceID)
// 	if err != nil && !cache.GetDB().IsNilErr(err) {
// 		log.Errorf("isTaskRunnning %s GetRunningDataTask err:%s", hash, err.Error())
// 		return false, err
// 	}

// 	if deviceID == "" {
// 		return cID != "", nil
// 	}

// 	return cID == deviceID, nil
// }

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

	err = saveEvent(cid, cacheID, "", fmt.Sprintf("expiredTime:%s", expiredTime.String()), eventTypeResetCacheTime)
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
		err = cache.GetDB().SetWaitingDataTask(&api.CarfileRecordInfo{CarfileHash: carfileHash, CarfileCid: info.CarfileCid, NeedReliability: info.NeedReliability, ExpiredTime: info.ExpiredTime})
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
	//TODO Check the expiration policy to be adjusted
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
		err = m.removeCache(cacheInfo, carfileCid)
		if err != nil {
			err = saveEvent(carfileCid, cacheInfo.DeviceID, "expired", err.Error(), eventTypeRemoveCache)
		} else {
			err = saveEvent(carfileCid, cacheInfo.DeviceID, "expired", "", eventTypeRemoveCache)
		}
	}

	m.isLoadExpiredTime = true
}

func (m *Manager) removeCache(cacheInfo *api.CacheTaskInfo, carfileCid string) error {
	blocks, err := m.notifyNodeRemoveCarfile(cacheInfo.DeviceID, carfileCid, cacheInfo.DoneBlocks)
	if err != nil {
		return err
	}

	err = cache.GetDB().SetCacheEnd(cacheInfo.CarfileHash, cacheInfo.DeviceID)
	if err != nil {
		log.Errorf("removeCache SetCacheEnd err: %s", err.Error())
	}

	// update node block count
	err = cache.GetDB().IncrByDeviceInfo(cacheInfo.DeviceID, cache.BlockCountField, -int64(blocks))
	if err != nil {
		log.Errorf("IncrByDeviceInfo err:%s ", err.Error())
	}

	reducingReliability := 0
	if cacheInfo.Status == api.CacheStatusSuccess {
		reducingReliability += cacheInfo.Reliability
		err = cache.GetDB().IncrByBaseInfo(cache.CarFileCountField, -1)
		if err != nil {
			log.Errorf("removeCache IncrByBaseInfo err:%s", err.Error())
		}
	}

	// delete cache and update data info
	err = persistent.GetDB().RemoveCacheAndUpdateData(cacheInfo.DeviceID, cacheInfo.CarfileHash, reducingReliability)

	dI, ok := m.CarfileRecordMap.Load(cacheInfo.CarfileHash)
	if ok && dI != nil {
		dI.(*CarfileRecord).CacheTaskMap.Delete(cacheInfo.DeviceID)
	}

	return err
}

// Notify nodes to delete blocks
func (m *Manager) notifyNodeRemoveCarfile(deviceID, cid string, recordBlocks int) (int, error) {
	edge := m.nodeManager.GetEdgeNode(deviceID)
	if edge != nil {
		return 0, edge.GetAPI().DeleteCarfile(context.Background(), cid)
	}

	candidate := m.nodeManager.GetCandidateNode(deviceID)
	if candidate != nil {
		return 0, candidate.GetAPI().DeleteCarfile(context.Background(), cid)
	}

	return recordBlocks, nil
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
func (m *Manager) findAppropriateEdges(cacheMap sync.Map, count int) []*node.EdgeNode {
	list := make([]*node.EdgeNode, 0)

	m.nodeManager.EdgeNodeMap.Range(func(key, value interface{}) bool {
		deviceID := key.(string)

		if _, exist := cacheMap.Load(deviceID); exist {
			return true
		}

		node := value.(*node.EdgeNode)
		if node.DiskUsage > diskUsageMax {
			return true
		}

		list = append(list, node)
		return true
	})

	sort.Slice(list, func(i, j int) bool {
		return list[i].GetCurCacheCount() < list[j].GetCurCacheCount()
	})

	return list[0:count]
}

// find the candidates
func (m *Manager) findAppropriateCandidates(cacheMap sync.Map, count int) []*node.CandidateNode {
	list := make([]*node.CandidateNode, 0)

	m.nodeManager.CandidateNodeMap.Range(func(key, value interface{}) bool {
		deviceID := key.(string)

		if _, exist := cacheMap.Load(deviceID); exist {
			return true
		}

		node := value.(*node.CandidateNode)
		if node.DiskUsage > diskUsageMax {
			return true
		}

		list = append(list, node)
		return true
	})

	sort.Slice(list, func(i, j int) bool {
		return list[i].GetCurCacheCount() < list[j].GetCurCacheCount()
	})

	return list[0:count]
}

func saveEvent(cid, cacheID, userID, msg string, event EventType) error {
	return persistent.GetDB().SetEventInfo(&api.EventInfo{CID: cid, User: userID, Msg: msg, Event: string(event), CacheID: cacheID})
}
