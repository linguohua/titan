package scheduler

import (
	"strings"
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

	runningTaskMap sync.Map

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
	// go d.startDataTask()

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
	list, err := cache.GetDB().GetTasksWithRunningList()
	if err != nil {
		return xerrors.Errorf("GetTasksWithRunningList err:%s", err.Error())
	}
	// log.Warnf("doDataTask running list:%v", list)
	if len(list) >= m.runningTaskMax {
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
		err = m.startCacheData(info.Cid, info.NeedReliability)
		if err != nil {
			return xerrors.Errorf("cid:%s,reliability:%d ; startCacheData err:%s", info.Cid, info.NeedReliability, err.Error())
		}
	}

	return nil
}

func (m *DataManager) findData(cid string, isStore bool) *Data {
	dI, ok := m.runningTaskMap.Load(cid)
	if ok && dI != nil {
		return dI.(*Data)
	}

	data := loadData(cid, m.nodeManager, m)
	if data != nil {
		if isStore {
			m.runningTaskMap.Store(cid, data)
		}
		return data
	}

	return nil
}

func (m *DataManager) checkTaskTimeout(cid, cacheID string) {
	cID, err := cache.GetDB().GetRunningTask(cid)
	if err != nil && !cache.GetDB().IsNilErr(err) {
		log.Errorf("checkTaskTimeout %s,%s GetRunningTask err:%s", cid, cacheID, err.Error())
		return
	}
	if cID != "" {
		return
	}

	defer func() {
		// log.Warnf("checkTaskTimeout remove runing llist:%s", cid)
		err = cache.GetDB().RemoveTaskWithRunningList(cid, cacheID)
		if err != nil {
			log.Errorf("checkTaskTimeout %s,%s RemoveTaskWithRunningList err:%s", cid, cacheID, err.Error())
		}
	}()

	// task timeout
	data := m.findData(cid, true)
	if data != nil {
		value, ok := data.cacheMap.Load(cacheID)
		if !ok {
			return
		}

		c := value.(*Cache)

		unDoneBlocks, err := persistent.GetDB().GetBloackCountWhitStatus(c.cacheID, int(cacheStatusCreate))
		if err != nil {
			log.Errorf("checkTaskTimeout %s,%s GetBloackCountWhitStatus err:%s", c.carfileCid, c.cacheID, err.Error())
			return
		}

		err = c.endCache(unDoneBlocks)
		if err != nil {
			log.Errorf("checkTaskTimeout %s", err.Error())
		}
	}
}

func (m *DataManager) checkTaskTimeouts() {
	list, err := cache.GetDB().GetTasksWithRunningList()
	if err != nil {
		log.Errorf("GetTasksWithList err:%s", err.Error())
		return
	}

	if list == nil || len(list) <= 0 {
		return
	}

	for _, cKey := range list {
		cidList := strings.Split(cKey, ":")
		if len(cidList) != 2 {
			continue
		}
		cid := cidList[0]
		cacheID := cidList[1]

		m.checkTaskTimeout(cid, cacheID)
	}
}

func (m *DataManager) startCacheData(cid string, reliability int) error {
	var err error
	isSave := false
	data := m.findData(cid, true)
	if data == nil {
		isSave = true
		data = newData(m.nodeManager, m, cid, reliability)

		m.runningTaskMap.Store(cid, data)
	}

	if data.needReliability != reliability {
		data.needReliability = reliability
		isSave = true
	}

	defer func() {
		if err != nil {
			m.dataTaskEnd(data.cid)
		}
	}()

	if isSave {
		err = persistent.GetDB().SetDataInfo(&persistent.DataInfo{
			CID:             data.cid,
			TotalSize:       data.totalSize,
			NeedReliability: data.needReliability,
			Reliability:     data.reliability,
			CacheCount:      data.cacheCount,
			TotalBlocks:     data.totalBlocks,
			RootCacheID:     data.rootCacheID,
		})
		if err != nil {
			return xerrors.Errorf("cid:%s,SetDataInfo err:%s", data.cid, err.Error())
		}
	}

	if data.needReliability <= data.reliability {
		return xerrors.Errorf("reliability is enough:%d/%d", data.reliability, data.needReliability)
	}

	// old cache
	var unDoneCache *Cache
	data.cacheMap.Range(func(key, value interface{}) bool {
		c := value.(*Cache)

		if c.status != cacheStatusSuccess {
			unDoneCache = c
			return true
		}

		return true
	})

	if unDoneCache != nil {
		err = data.cacheContinue(unDoneCache.cacheID)
		if err != nil {
			err = xerrors.Errorf("cacheContinue err:%s", err.Error())
		}
	} else {
		// create cache again
		err = data.startData()
		if err != nil {
			err = xerrors.Errorf("startData err:%s", err.Error())
		}
	}

	return err
}

func (m *DataManager) startCacheContinue(cid, cacheID string) error {
	data := m.findData(cid, true)
	if data == nil {
		return xerrors.Errorf("%s,cid:%s,cacheID:%s", ErrNotFoundTask, cid, cacheID)
	}

	err := data.cacheContinue(cacheID)
	if err != nil {
		m.dataTaskEnd(data.cid)
		return err
	}

	return nil
}

func (m *DataManager) cacheData(cid string, reliability int) error {
	return cache.GetDB().SetWaitingCacheTask(api.CacheDataInfo{Cid: cid, NeedReliability: reliability})
	// return m.startCacheData(cid, reliability)
}

func (m *DataManager) cacheContinue(cid, cacheID string) error {
	return cache.GetDB().SetWaitingCacheTask(api.CacheDataInfo{Cid: cid, CacheInfos: []api.CacheInfo{{CacheID: cacheID}}})
	// return m.startCacheContinue(cid, cacheID)
}

func (m *DataManager) removeCarfile(carfileCid string) error {
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
	data := m.findData(carfileCid, false)
	if data == nil {
		return xerrors.Errorf("%s : %s", ErrNotFoundTask, carfileCid)
	}

	cacheI, ok := data.cacheMap.Load(cacheID)
	if !ok {
		return xerrors.Errorf("removeCache not found cacheID:%s,Cid:%s", cacheID, data.cid)
	}
	cache := cacheI.(*Cache)

	err := cache.removeCache()
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
	cache := cacheI.(*Cache)

	return cache.blockCacheResult(info)
}

func (m *DataManager) dataTaskEnd(cid string) {
	m.runningTaskMap.Delete(cid)
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
