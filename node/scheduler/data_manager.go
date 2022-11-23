package scheduler

import (
	"fmt"
	"sync"
	"time"

	"github.com/linguohua/titan/api"
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
		blockLoaderCh:    make(chan bool),
		dataTaskLoaderCh: make(chan bool),
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
			m.doResultTasks()
		}
	}
}

func (m *DataManager) getWaitingTask(index int64) (api.CacheDataInfo, error) {
	info, err := cache.GetDB().GetWaitingCacheTask(index)
	if err != nil {
		return info, err
	}

	if m.isTaskRunnning(info.CarfileCid, "") {
		info, err = m.getWaitingTask(index + 1)
		if err != nil {
			return info, err
		}
	}

	return info, nil
}

func (m *DataManager) doDataTask() error {
	info, err := m.getWaitingTask(0)
	if err != nil {
		if cache.GetDB().IsNilErr(err) {
			return nil
		}
		return xerrors.Errorf("getWaitingTask err:%s", err.Error())
	}

	cacheID := ""
	defer func() {
		if err != nil {
			m.saveEvent(info.CarfileCid, cacheID, "", err.Error(), eventTypeDoDataTaskErr)
		}

		err = cache.GetDB().RemoveWaitingCacheTask(info)
		if err != nil {
			log.Errorf("RemoveWaitingCacheTask err:%s", err.Error())
		}
	}()

	if info.CacheInfos != nil && len(info.CacheInfos) > 0 {
		cacheID = info.CacheInfos[0].CacheID

		err = m.makeDataContinue(info.CarfileCid, cacheID)
		if err != nil {
			return xerrors.Errorf("makeDataContinue err:%s", err.Error())
		}
	} else {
		cacheID, err = m.makeDataTask(info.CarfileCid, info.NeedReliability, info.ExpiredTime)
		if err != nil {
			return xerrors.Errorf("makeDataTask err:%s", err.Error())
		}
	}

	return nil
}

func (m *DataManager) findData(cid string) *Data {
	dI, ok := m.taskMap.Load(cid)
	if ok && dI != nil {
		return dI.(*Data)
	}

	data := loadData(cid, m.nodeManager, m)
	if data != nil {
		return data
	}

	return nil
}

func (m *DataManager) checkTaskTimeout(taskInfo cache.DataTask) {
	if m.isTaskRunnning(taskInfo.CarfileCid, taskInfo.CacheID) {
		return
	}

	data := m.findData(taskInfo.CarfileCid)
	if data == nil {
		return
	}

	cI, ok := data.cacheMap.Load(taskInfo.CacheID)
	if ok && cI != nil {
		cache := cI.(*Cache)
		err := cache.stopCache(0, true)
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

func (m *DataManager) makeDataTask(cid string, reliability int, expiredTime time.Time) (string, error) {
	var err error
	data := m.findData(cid)
	if data == nil {
		data = newData(m.nodeManager, m, cid, reliability)
		data.expiredTime = expiredTime
	} else {
		if reliability <= data.reliability {
			return "", xerrors.Errorf("reliability is enough:%d/%d", data.reliability, reliability)
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
	})
	if err != nil {
		return "", xerrors.Errorf("cid:%s,SetDataInfo err:%s", data.carfileCid, err.Error())
	}

	// old cache
	cacheID := ""
	data.cacheMap.Range(func(key, value interface{}) bool {
		c := value.(*Cache)

		if c.status != persistent.CacheStatusSuccess {
			cacheID = c.cacheID
			return true
		}

		return true
	})

	data.cacheCount = data.reliability

	cacheID, err = data.dispatchCache(cacheID)
	if err != nil {
		return cacheID, err
	}

	m.dataTaskStart(data)
	return cacheID, nil
}

func (m *DataManager) makeDataContinue(cid, cacheID string) error {
	data := m.findData(cid)
	if data == nil {
		return xerrors.Errorf("%s,cid:%s,cacheID:%s", ErrNotFoundTask, cid, cacheID)
	}

	data.cacheCount = data.reliability

	_, err := data.dispatchCache(cacheID)
	if err != nil {
		return err
	}

	m.dataTaskStart(data)
	return nil
}

func (m *DataManager) cacheData(cid string, reliability int, expiredTime int) error {
	t := time.Now().Add(time.Duration(expiredTime) * time.Hour)

	err := cache.GetDB().SetWaitingCacheTask(api.CacheDataInfo{CarfileCid: cid, NeedReliability: reliability, ExpiredTime: t})
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
	err := cache.GetDB().SetWaitingCacheTask(api.CacheDataInfo{CarfileCid: cid, CacheInfos: []api.CacheInfo{{CacheID: cacheID}}})
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
		log.Errorf("cacheID:%s, removeBlocks err:%s", cache.cacheID, err.Error())
	}

	return m.saveEvent(carfileCid, cacheID, "user", e, eventTypeRemoveCacheEnd)
}

func (m *DataManager) cacheCarfileResult(deviceID string, info *api.CacheResultInfo) error {
	// area := m.nodeManager.getNodeArea(deviceID)
	data := m.findData(info.CarFileCid)
	if data == nil {
		return xerrors.Errorf("%s : %s", ErrNotFoundTask, info.CarFileCid)
	}

	if !m.isTaskRunnning(info.CarFileCid, info.CacheID) {
		return xerrors.Errorf("%s : %s", ErrNotFoundTask, info.CarFileCid)
	}

	cacheI, ok := data.cacheMap.Load(info.CacheID)
	if !ok {
		return xerrors.Errorf("cacheCarfileResult not found cacheID:%s,Cid:%s", info.CacheID, data.carfileCid)
	}
	c := cacheI.(*Cache)

	return c.blockCacheResult(info)
}

func (m *DataManager) doResultTasks() {
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

func (m *DataManager) updateDataTimeout(carfileCid, cacheID string, timeout int64) {
	err := cache.GetDB().SetRunningTask(carfileCid, cacheID, timeout)
	if err != nil {
		log.Panicf("dataTaskStart %s , SetRunningTask err:%s", cacheID, err.Error())
	}
}

func (m *DataManager) dataTaskStart(data *Data) {
	if data == nil {
		log.Error("dataTaskStart err data is nil")
		return
	}
	m.saveEvent(data.carfileCid, "", "", "", eventTypeDoDataTaskStart)
	// log.Infof("taskStart:%s ---------- ", data.cid)

	m.taskMap.Store(data.carfileCid, data)
}

func (m *DataManager) dataTaskEnd(cid, msg string) {
	m.saveEvent(cid, "", "", msg, eventTypeDoDataTaskEnd)
	// log.Infof("taskEnd:%s ---------- ", cid)
	m.taskMap.Delete(cid)

	// continue task
	m.notifyDataLoader()
}

func (m *DataManager) getRunningTasks() []cache.DataTask {
	list, err := cache.GetDB().GetTasksWithRunningList()
	if err != nil {
		log.Errorf("GetTasksWithRunningList err:%s", err.Error())
	}

	return list
}

func (m *DataManager) isTaskRunnning(cid, cacheID string) bool {
	cID, err := cache.GetDB().GetRunningTask(cid)
	if err != nil && !cache.GetDB().IsNilErr(err) {
		log.Errorf("isTaskRunnning %s GetRunningTask err:%s", cid, err.Error())
		return true
	}

	if cacheID == "" {
		return cID != ""
	}

	return cID == cacheID
}

func (m *DataManager) saveEvent(cid, cacheID, userID, msg string, event EventType) error {
	return persistent.GetDB().SetEventInfo(&persistent.EventInfo{CID: cid, User: userID, Msg: msg, Event: string(event), CacheID: cacheID})
}
