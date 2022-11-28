package scheduler

import (
	"fmt"
	"sync"
	"time"

	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/node/helper"
	"github.com/linguohua/titan/node/scheduler/db/cache"
	"github.com/linguohua/titan/node/scheduler/db/persistent"
	"golang.org/x/xerrors"
)

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
)

// DataManager Data
type DataManager struct {
	nodeManager      *NodeManager
	blockLoaderCh    chan bool
	dataTaskLoaderCh chan bool

	taskMap sync.Map

	timerInterval int //  time interval (Second)

	runningTaskMax int
}

func newDataManager(nodeManager *NodeManager) *DataManager {
	d := &DataManager{
		nodeManager:      nodeManager,
		blockLoaderCh:    make(chan bool, 1),
		dataTaskLoaderCh: make(chan bool, 1),
		timerInterval:    30,
		runningTaskMax:   5,
	}

	go d.run()

	return d
}

func (m *DataManager) run() {
	ticker := time.NewTicker(time.Duration(m.timerInterval) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			m.checkTaskTimeouts()
			m.notifyDataLoader()
			// check data expired TODO
		case <-m.dataTaskLoaderCh:
			m.doDataTasks()
		case <-m.blockLoaderCh:
			m.doCacheResults()
		}
	}
}

func (m *DataManager) getWaitingDataTask(index int64) (api.CacheDataInfo, error) {
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

func (m *DataManager) doDataTask() error {
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
				cacheID = c.cacheID
			}
			m.saveEvent(info.CarfileCid, cacheID, "", err.Error(), eventTypeDoDataTaskErr)
		} else {
			m.dataTaskStart(c.data)
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

func (m *DataManager) findData(hash string) *Data {
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

func (m *DataManager) checkTaskTimeout(taskInfo cache.DataTask) {
	if m.isDataTaskRunnning(taskInfo.CarfileHash, taskInfo.CacheID) {
		return
	}

	data := m.findData(taskInfo.CarfileHash)
	if data == nil {
		return
	}

	cI, ok := data.cacheMap.Load(taskInfo.CacheID)
	if ok && cI != nil {
		cache := cI.(*Cache)
		err := cache.endCache(0, true)
		if err != nil {
			log.Errorf("stopCache err:%s", err.Error())
		}
	}
}

func (m *DataManager) checkTaskTimeouts() {
	list := m.getRunningTasks()
	if list == nil || len(list) <= 0 {
		return
	}

	for _, taskInfo := range list {
		m.checkTaskTimeout(taskInfo)
	}
}

func (m *DataManager) makeDataTask(cid, hash string, reliability int, expiredTime time.Time) (*Cache, error) {
	var err error
	data := m.findData(cid)
	if data == nil {
		data = newData(m.nodeManager, m, cid, hash, reliability)
		data.expiredTime = expiredTime
	} else {
		if reliability <= data.reliability {
			return nil, xerrors.Errorf("reliability is enough:%d/%d", data.reliability, reliability)
		}
		data.needReliability = reliability
		// TODO expiredTime
	}

	// log.Warnf("askCacheData reliability:%d,data.needReliability:%d,data.reliability:%d", reliability, data.needReliability, data.reliability)

	err = persistent.GetDB().SetDataInfo(&persistent.DataInfo{
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
		return nil, xerrors.Errorf("cid:%s,SetDataInfo err:%s", data.carfileCid, err.Error())
	}

	// old cache
	var oldCache *Cache
	data.cacheMap.Range(func(key, value interface{}) bool {
		c := value.(*Cache)

		if c.status != persistent.CacheStatusSuccess {
			oldCache = c
		}

		return true
	})

	data.cacheCount = data.reliability

	return data.dispatchCache(oldCache)
}

func (m *DataManager) makeDataContinue(hash, cacheID string) (*Cache, error) {
	data := m.findData(hash)
	if data == nil {
		return nil, xerrors.Errorf("%s,cid:%s,cacheID:%s", ErrNotFoundTask, hash, cacheID)
	}

	cacheI, ok := data.cacheMap.Load(cacheID)
	if !ok || cacheI == nil {
		return nil, xerrors.Errorf("Not Found CacheID :%s", cacheID)
	}
	cache := cacheI.(*Cache)

	data.cacheCount = data.reliability

	return data.dispatchCache(cache)
}

func (m *DataManager) cacheData(cid string, reliability int, expiredTime int) error {
	hash, err := helper.CIDString2HashString(cid)
	if err != nil {
		return xerrors.Errorf("%s cid to hash err:", cid, err.Error())
	}

	t := time.Now().Add(time.Duration(expiredTime) * time.Hour)

	err = cache.GetDB().SetWaitingDataTask(api.CacheDataInfo{CarfileHash: hash, CarfileCid: cid, NeedReliability: reliability, ExpiredTime: t})
	if err != nil {
		return err
	}

	err = m.saveEvent(cid, "", "user", fmt.Sprintf("reliability:%d", reliability), eventTypeAddNewDataTask)
	if err != nil {
		return err
	}

	m.notifyDataLoader()

	return nil
}

func (m *DataManager) cacheContinue(cid, cacheID string) error {
	hash, err := helper.CIDString2HashString(cid)
	if err != nil {
		return xerrors.Errorf("%s cid to hash err:", cid, err.Error())
	}

	err = cache.GetDB().SetWaitingDataTask(api.CacheDataInfo{CarfileHash: hash, CarfileCid: cid, CacheInfos: []api.CacheInfo{{CacheID: cacheID}}})
	if err != nil {
		return err
	}

	err = m.saveEvent(cid, cacheID, "user", "", eventTypeAddContinueDataTask)
	if err != nil {
		return err
	}

	m.notifyDataLoader()

	return nil
}

func (m *DataManager) removeCarfile(carfileCid string) error {
	err := m.saveEvent(carfileCid, "", "user", "", eventTypeRemoveDataStart)
	if err != nil {
		return err
	}

	data := m.findData(carfileCid)
	if data == nil {
		return xerrors.Errorf("%s : %s", ErrNotFoundTask, carfileCid)
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

func (m *DataManager) removeCache(carfileCid, cacheID string) error {
	err := m.saveEvent(carfileCid, cacheID, "user", "", eventTypeRemoveCacheStart)
	if err != nil {
		return err
	}

	data := m.findData(carfileCid)
	if data == nil {
		return xerrors.Errorf("%s : %s", ErrNotFoundTask, carfileCid)
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

func (m *DataManager) cacheCarfileResult(deviceID string, info *api.CacheResultInfo) error {
	// area := m.nodeManager.getNodeArea(deviceID)
	data := m.findData(info.CarFileHash)
	if data == nil {
		return xerrors.Errorf("%s : %s", ErrNotFoundTask, info.CarFileHash)
	}

	if !m.isDataTaskRunnning(info.CarFileHash, info.CacheID) {
		return xerrors.Errorf("%s : %s", ErrNotFoundTask, info.CarFileHash)
	}

	cacheI, ok := data.cacheMap.Load(info.CacheID)
	if !ok {
		return xerrors.Errorf("cacheCarfileResult not found cacheID:%s,Cid:%s", info.CacheID, data.carfileCid)
	}
	c := cacheI.(*Cache)

	return c.blockCacheResult(info)
}

func (m *DataManager) doCacheResults() {
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

		err = cache.GetDB().RemoveCacheResultInfo()
		if err != nil {
			log.Errorf("doResultTask RemoveCacheResultInfo err:%s", err.Error())
			return
		}
	}
}

func (m *DataManager) pushCacheResultToQueue(deviceID string, info *api.CacheResultInfo) error {
	info.DeviceID = deviceID

	err := cache.GetDB().SetCacheResultInfo(*info)

	m.notifyBlockLoader()

	return err
}

func (m *DataManager) notifyBlockLoader() {
	select {
	case m.blockLoaderCh <- true:
	default:
	}
}

func (m *DataManager) doDataTasks() {
	doLen := m.runningTaskMax - len(m.getRunningTasks())
	if doLen > 0 {
		for i := 0; i < doLen; i++ {
			err := m.doDataTask()
			if err != nil {
				log.Errorf("doDataTask er:%s", err.Error())
			}
		}
	}
}

func (m *DataManager) notifyDataLoader() {
	select {
	case m.dataTaskLoaderCh <- true:
	default:
	}
}

// update the data task timeout
func (m *DataManager) updateDataTimeout(carfileHash, cacheID string, timeout int64) {
	err := cache.GetDB().SetRunningDataTask(carfileHash, cacheID, timeout)
	if err != nil {
		log.Panicf("dataTaskStart %s , SetRunningDataTask err:%s", cacheID, err.Error())
	}
}

func (m *DataManager) dataTaskStart(data *Data) {
	if data == nil {
		log.Error("dataTaskStart err data is nil")
		return
	}
	m.saveEvent(data.carfileCid, "", "", "", eventTypeDoDataTaskStart)

	m.taskMap.Store(data.carfileCid, data)
}

func (m *DataManager) dataTaskEnd(cid, msg string) {
	m.saveEvent(cid, "", "", msg, eventTypeDoDataTaskEnd)

	m.taskMap.Delete(cid)

	// continue task
	m.notifyDataLoader()
}

func (m *DataManager) getRunningTasks() []cache.DataTask {
	list, err := cache.GetDB().GetDataTasksWithRunningList()
	if err != nil {
		log.Errorf("GetDataTasksWithRunningList err:%s", err.Error())
	}

	return list
}

func (m *DataManager) isDataTaskRunnning(hash, cacheID string) bool {
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

func (m *DataManager) saveEvent(cid, cacheID, userID, msg string, event EventType) error {
	return persistent.GetDB().SetEventInfo(&api.EventInfo{CID: cid, User: userID, Msg: msg, Event: string(event), CacheID: cacheID})
}

// replenish time

// stop
func (m *DataManager) stopDataTask(cid string) {
	m.saveEvent(cid, "", "user", "", eventTypeStopDataTask)

	// if data unstart

	data := m.findData(cid)
	data.isStop = true
}

// expired
func (m *DataManager) checkCacheExpired(cid string) {
	now := time.Now()
	data := m.findData(cid)

	data.cacheMap.Range(func(key, value interface{}) bool {
		c := value.(*Cache)
		if c.expiredTime.After(now) {
			return true
		}

		// do remove
		err := c.removeCache()
		if err != nil {
			m.saveEvent(cid, c.cacheID, "expired", err.Error(), eventTypeRemoveCacheEnd)
		} else {
			m.saveEvent(cid, c.cacheID, "expired", "", eventTypeRemoveCacheEnd)
		}

		return true
	})
}
