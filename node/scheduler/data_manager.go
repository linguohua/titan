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

	taskTimeWheel *timewheel.TimeWheel
	taskTimeout   int // keepalive time interval (minute)

	runningMax int
}

func newDataManager(nodeManager *NodeManager) *DataManager {
	d := &DataManager{
		nodeManager:   nodeManager,
		blockLoaderCh: make(chan bool),
		taskTimeout:   1,
		runningMax:    10,
	}

	d.initTimewheel()
	go d.startBlockLoader()
	// go d.startDataTask()

	return d
}

func (m *DataManager) initTimewheel() {
	m.taskTimeWheel = timewheel.New(1*time.Second, 3600, func(_ interface{}) {
		m.taskTimeWheel.AddTimer((time.Duration(m.taskTimeout)*60-1)*time.Second, "TaskTimeout", nil)
		m.checkTaskTimeout()
	})
	m.taskTimeWheel.Start()
	m.taskTimeWheel.AddTimer((time.Duration(m.taskTimeout)*60-1)*time.Second, "TaskTimeout", nil)
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

func (m *DataManager) checkTaskTimeout() {
	list, err := cache.GetDB().GetTasksWithList()
	if err != nil {
		log.Errorf("GetTasksWithList err:%s", err.Error())
		return
	}

	if list == nil || len(list) <= 0 {
		return
	}

	for _, cKey := range list {
		cidList := strings.Split(cKey, ";")
		if len(cidList) != 2 {
			continue
		}
		cid := cidList[0]
		cacheID := cidList[1]

		cID, _ := cache.GetDB().GetRunningCacheTask(cid)
		if cID != "" {
			continue
		}

		// task timeout
		data := m.findData(cid, true)
		value, ok := data.cacheMap.Load(cacheID)
		if !ok {
			continue
		}

		c := value.(*Cache)

		unDoneBlocks, err := persistent.GetDB().HaveBlocks(c.cacheID, int(cacheStatusCreate))
		if err != nil {
			log.Errorf("checkTaskTimeout %s,%s HaveBlocks err:%v", c.carfileCid, c.cacheID, err)
			continue
		}

		err = c.endCache(unDoneBlocks)
		if err != nil {
			log.Errorf("checkTaskTimeout %s", err.Error())
		}
	}
}

func (m *DataManager) startCacheData(cid string, reliability int) error {
	var err error
	isSave := false
	data := m.findData(cid, true)
	if data == nil {
		isSave = true
		data = newData(m.nodeManager, m, cid, reliability)
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
			CacheIDs:        data.cacheIDs,
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

	return data.startData()
}

func (m *DataManager) cacheData(cid string, reliability int) error {
	list, err := cache.GetDB().GetTasksWithList()
	if err != nil {
		return err
	}

	if len(list) > m.runningMax {
		err = cache.GetDB().SetWaitingCacheTask(api.CacheDataInfo{Cid: cid, NeedReliability: reliability})
		if err != nil {
			return err
		}
	}

	return m.startCacheData(cid, reliability)
}

func (m *DataManager) cacheContinue(cid, cacheID string) error {
	data := m.findData(cid, true)
	if data == nil {
		return xerrors.Errorf("%s,cid:%s,cacheID:%s", ErrNotFoundTask, cid, cacheID)
	}

	return data.cacheContinue(cacheID)
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

	// next data task
	info, err := cache.GetDB().GetWaitingCacheTask()
	if err != nil {
		log.Errorf("GetWaitingCacheTask err:%s", err.Error())
		return
	}

	err = m.startCacheData(info.Cid, info.NeedReliability)
	if err != nil {
		log.Errorf("startCacheData err:%s", err.Error())
		return
	}

	err = cache.GetDB().RemoveWaitingCacheTask()
	if err != nil {
		log.Errorf("RemoveWaitingCacheTask err:%s", err.Error())
		return
	}
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
