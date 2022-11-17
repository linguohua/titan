package scheduler

import (
	"sync"
	"time"

	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/node/scheduler/db/cache"
	"github.com/linguohua/titan/node/scheduler/db/persistent"
	"github.com/ouqiang/timewheel"
	"golang.org/x/xerrors"
)

// DataManager Data
type DataManager struct {
	nodeManager    *NodeManager
	blockLoaderCh  chan bool
	waitingCacheCh chan bool

	taskMap sync.Map

	timeoutTimeWheel *timewheel.TimeWheel
	timeoutTime      int // check timeout time interval (minute)

	taskTimeWheel *timewheel.TimeWheel
	doTaskTime    int // task time interval (Second)

	runningTaskMax int
}

func newDataManager(nodeManager *NodeManager) *DataManager {
	d := &DataManager{
		nodeManager:    nodeManager,
		blockLoaderCh:  make(chan bool),
		timeoutTime:    1,
		doTaskTime:     30,
		runningTaskMax: 1,
	}

	d.initTimewheel()
	go d.startBlockLoader()

	return d
}

func (m *DataManager) initTimewheel() {
	m.timeoutTimeWheel = timewheel.New(1*time.Second, 3600, func(_ interface{}) {
		m.timeoutTimeWheel.AddTimer((time.Duration(m.timeoutTime)*60-1)*time.Second, "TaskTimeout", nil)
		m.checkTaskTimeouts()
	})
	m.timeoutTimeWheel.Start()
	m.timeoutTimeWheel.AddTimer((time.Duration(m.timeoutTime)*60-1)*time.Second, "TaskTimeout", nil)

	m.taskTimeWheel = timewheel.New(1*time.Second, 3600, func(_ interface{}) {
		m.taskTimeWheel.AddTimer(time.Duration(m.doTaskTime-1)*time.Second, "DataTask", nil)
		err := m.doDataTask()
		if err != nil {
			log.Errorf("doTask err :%s", err.Error())
		}
	})
	m.taskTimeWheel.Start()
	m.taskTimeWheel.AddTimer(time.Duration(m.doTaskTime-1)*time.Second, "DataTask", nil)
}

func (m *DataManager) doDataTask() error {
	if len(m.getRunningTasks()) > 0 {
		return nil
	}

	// next data task
	info, err := cache.GetDB().GetWaitingCacheTask()
	if err != nil {
		if cache.GetDB().IsNilErr(err) {
			return nil
		}
		return xerrors.Errorf("GetWaitingCacheTask err:%s", err.Error())
	}

	defer func() {
		err = cache.GetDB().RemoveWaitingCacheTask()
		if err != nil {
			log.Errorf("cid:%s ; RemoveWaitingCacheTask err:%s", info.Cid, err.Error())
		}
	}()

	if info.CacheInfos != nil && len(info.CacheInfos) > 0 {
		cacheID := info.CacheInfos[0].CacheID

		err = m.startCacheContinue(info.Cid, cacheID)
		if err != nil {
			return xerrors.Errorf("cid:%s,cacheID:%s ; startCacheContinue err:%s", info.Cid, cacheID, err.Error())
		}
	} else {
		err = m.startCacheData(info.Cid, info.NeedReliability, info.ExpiredTime)
		if err != nil {
			return xerrors.Errorf("cid:%s,reliability:%d ; startCacheData err:%s", info.Cid, info.NeedReliability, err.Error())
		}
	}

	return nil
}

func (m *DataManager) findData(cid string, isStore bool) *Data {
	dI, ok := m.taskMap.Load(cid)
	if ok && dI != nil {
		return dI.(*Data)
	}

	data := loadData(cid, m.nodeManager, m)
	if data != nil {
		if isStore {
			m.taskMap.Store(cid, data)
		}
		return data
	}

	return nil
}

func (m *DataManager) checkTaskTimeout(cid string) {
	if m.isTaskRunnning(cid) {
		return
	}

	m.dataTaskEnd(cid)
}

func (m *DataManager) checkTaskTimeouts() {
	list := m.getRunningTasks()
	if len(list) <= 0 {
		return
	}

	for _, cid := range list {
		m.checkTaskTimeout(cid)
	}
}

func (m *DataManager) startCacheData(cid string, reliability int, expiredTime time.Time) error {
	var err error
	isSave := false
	data := m.findData(cid, true)
	if data == nil {
		isSave = true
		data = newData(m.nodeManager, m, cid, reliability)

		m.taskMap.Store(cid, data)
	}

	if data.needReliability != reliability {
		data.needReliability = reliability
		isSave = true
	}

	if data.expiredTime != expiredTime {
		data.expiredTime = expiredTime
		isSave = true
	}

	// defer func() {
	// 	if err != nil {
	// 		m.dataTaskEnd(data.cid)
	// 	}
	// }()

	if isSave {
		err = persistent.GetDB().SetDataInfo(&persistent.DataInfo{
			CID:             data.cid,
			TotalSize:       data.totalSize,
			NeedReliability: data.needReliability,
			Reliability:     data.reliability,
			CacheCount:      data.cacheCount,
			TotalBlocks:     data.totalBlocks,
			RootCacheID:     data.rootCacheID,
			ExpiredTime:     data.expiredTime,
		})
		if err != nil {
			return xerrors.Errorf("cid:%s,SetDataInfo err:%s", data.cid, err.Error())
		}
	}

	if data.needReliability <= data.reliability {
		return xerrors.Errorf("reliability is enough:%d/%d", data.reliability, data.needReliability)
	}

	// old cache
	cacheID := ""
	data.cacheMap.Range(func(key, value interface{}) bool {
		c := value.(*Cache)

		if c.status != cacheStatusSuccess {
			cacheID = c.cacheID
			return true
		}

		return true
	})

	return data.startData(cacheID)
}

func (m *DataManager) startCacheContinue(cid, cacheID string) error {
	data := m.findData(cid, true)
	if data == nil {
		return xerrors.Errorf("%s,cid:%s,cacheID:%s", ErrNotFoundTask, cid, cacheID)
	}

	return data.startData(cacheID)
}

func (m *DataManager) cacheData(cid string, reliability int) error {
	t := time.Now().Add(7 * 24 * time.Hour) // TODO

	err := cache.GetDB().SetWaitingCacheTask(api.CacheDataInfo{Cid: cid, NeedReliability: reliability, ExpiredTime: t})
	if err != nil {
		return err
	}

	return m.callToEvent(cid, "", "", "cacheData")
}

func (m *DataManager) cacheContinue(cid, cacheID string) error {
	err := cache.GetDB().SetWaitingCacheTask(api.CacheDataInfo{Cid: cid, CacheInfos: []api.CacheInfo{{CacheID: cacheID}}})
	if err != nil {
		return err
	}

	return m.callToEvent(cid, "", "", "cacheContinue")
}

func (m *DataManager) removeCarfile(carfileCid string) error {
	err := m.callToEvent(carfileCid, "", "", "removeCarfile")
	if err != nil {
		return err
	}

	data := m.findData(carfileCid, false)
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

	return nil
}

func (m *DataManager) removeCache(carfileCid, cacheID string) error {
	err := m.callToEvent(carfileCid, "", "", "removeCache")
	if err != nil {
		return err
	}

	data := m.findData(carfileCid, false)
	if data == nil {
		return xerrors.Errorf("%s : %s", ErrNotFoundTask, carfileCid)
	}

	cacheI, ok := data.cacheMap.Load(cacheID)
	if !ok {
		return xerrors.Errorf("removeCache not found cacheID:%s,Cid:%s", cacheID, data.cid)
	}
	cache := cacheI.(*Cache)

	err = cache.removeCache()
	if err != nil {
		log.Errorf("cacheID:%s, removeBlocks err:%s", cache.cacheID, err.Error())
	}

	return nil
}

func (m *DataManager) cacheCarfileResult(deviceID string, info *api.CacheResultInfo) error {
	// area := m.nodeManager.getNodeArea(deviceID)
	data := m.findData(info.CarFileCid, true)

	if data == nil {
		return xerrors.Errorf("%s : %s", ErrNotFoundTask, info.CarFileCid)
	}

	cacheI, ok := data.cacheMap.Load(info.CacheID)
	if !ok {
		return xerrors.Errorf("cacheCarfileResult not found cacheID:%s,Cid:%s", info.CacheID, data.cid)
	}
	c := cacheI.(*Cache)

	err := cache.GetDB().SetRunningTask(c.carfileCid, info.CacheID)
	if err != nil {
		log.Errorf("cacheID:%s,%s SetRunningTask err:%s ", info.CacheID, info.Cid, err.Error())
	}

	return c.blockCacheResult(info)
}

func (m *DataManager) doResultTask() {
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

	// m.enqueue(info)
	m.notifyBlockLoader()

	return err
}

func (m *DataManager) startBlockLoader() {
	for {
		<-m.blockLoaderCh
		m.doResultTask()
	}
}

func (m *DataManager) notifyBlockLoader() {
	select {
	case m.blockLoaderCh <- true:
	default:
	}
}

func (m *DataManager) dataTaskStart(cid, cacheID string) {
	log.Infof("taskStart:%s ; cacheID:%s ---------- ", cid, cacheID)

	err := cache.GetDB().SetRunningTask(cid, cacheID)
	if err != nil {
		log.Panicf("dataTaskStart %s , SetRunningTask err:%s", cacheID, err.Error())
	}

	err = cache.GetDB().SetTaskToRunningList(cid)
	if err != nil {
		log.Panicf("dataTaskStart %s , SetTaskToRunningList err:%s", cacheID, err.Error())
	}
}

func (m *DataManager) dataTaskEnd(cid string) {
	log.Infof("taskEnd:%s ---------- ", cid)

	err := cache.GetDB().RemoveRunningTask(cid)
	if err != nil {
		err = xerrors.Errorf("dataTaskEnd RemoveRunningTask err: %s", err.Error())
		return
	}
}

func (m *DataManager) getRunningTasks() []string {
	list, err := cache.GetDB().GetTasksWithRunningList()
	if err != nil {
		log.Errorf("GetTasksWithRunningList err:%s", err.Error())
		return make([]string, 0)
	}

	return list
}

func (m *DataManager) isTaskRunnning(cid string) bool {
	cID, err := cache.GetDB().GetRunningTask(cid)
	if err != nil && !cache.GetDB().IsNilErr(err) {
		log.Errorf("isTaskRunnning %s GetRunningTask err:%s", cid, err.Error())
		return true
	}

	return cID != ""
}

func (m *DataManager) callToEvent(cid, deviceID, userID, event string) error {
	// TODO event

	return nil
}
