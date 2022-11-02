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

	runningTaskMap sync.Map

	taskTimeWheel *timewheel.TimeWheel
	taskTimeout   int // keepalive time interval (minute)
}

func newDataManager(nodeManager *NodeManager) *DataManager {
	d := &DataManager{
		nodeManager:   nodeManager,
		blockLoaderCh: make(chan bool),
	}

	d.initTimewheel()
	go d.startBlockLoader()
	// go d.startDataTask()

	return d
}

func (m *DataManager) initTimewheel() {
	m.taskTimeWheel = timewheel.New(1*time.Second, 3600, func(_ interface{}) {
		m.taskTimeWheel.AddTimer(time.Duration(m.taskTimeout)*time.Minute, "TaskTimeout", nil)
		m.checkTaskTimeout()
	})
	m.taskTimeWheel.Start()
	m.taskTimeWheel.AddTimer(time.Duration(m.taskTimeout)*time.Minute, "TaskTimeout", nil)
}

func (m *DataManager) findData(area, cid string, isStore bool) *Data {
	dI, ok := m.runningTaskMap.Load(cid)
	if ok && dI != nil {
		return dI.(*Data)
	}

	data := loadData(area, cid, m.nodeManager, m)
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

	for _, cid := range list {
		tCid, err := cache.GetDB().GetRunningCacheTask(cid)
		if err == nil && tCid == cid {
			continue
		}

		// task timeout
		data := m.findData(serverArea, cid, true)
		data.cacheMap.Range(func(key, value interface{}) bool {
			c := value.(*Cache)

			if c.status > cacheStatusCreate {
				return true
			}

			c.endCache()

			return true
		})
	}
}

func (m *DataManager) cacheData(area, cid string, reliability int) error {
	isSave := false
	data := m.findData(area, cid, true)
	if data == nil {
		isSave = true
		data = newData(area, m.nodeManager, m, cid, reliability)
	}

	if data.needReliability != reliability {
		data.needReliability = reliability
		isSave = true
	}

	if isSave {
		err := persistent.GetDB().SetDataInfo(&persistent.DataInfo{
			CID:             data.cid,
			CacheIDs:        data.cacheIDs,
			TotalSize:       data.totalSize,
			NeedReliability: data.needReliability,
			Reliability:     data.reliability,
			CacheTime:       data.cacheTime,
			TotalBlocks:     data.totalBlocks,
			RootCacheID:     data.rootCacheID,
		})
		if err != nil {
			log.Errorf("cid:%s,SetDataInfo err:%s", data.cid, err.Error())
			return err
		}
	}

	return data.startData()
}

func (m *DataManager) cacheContinue(area, cid, cacheID string) error {
	data := m.findData(area, cid, true)
	if data == nil {
		return xerrors.Errorf("%s,cid:%s,cacheID:%s", ErrNotFoundTask, cid, cacheID)
	}

	return data.cacheContinue(cacheID)
}

func (m *DataManager) removeCarfile(carfileCid string) error {
	// TODO removeCarfile data info
	log.Errorf("removeCarfile carfileCid:%s", carfileCid)

	data := m.findData(serverArea, carfileCid, false)
	if data == nil {
		return xerrors.Errorf("%s : %s", ErrNotFoundTask, carfileCid)
	}

	return nil
}

func (m *DataManager) removeCache(carfileCid, cacheID string) error {
	// TODO removeCarfile data info
	log.Errorf("removeCache carfileCid:%s,cacheID:%v", carfileCid, cacheID)

	return nil
}

func (m *DataManager) cacheCarfileResult(deviceID string, info *api.CacheResultInfo) error {
	area := m.nodeManager.getNodeArea(deviceID)
	data := m.findData(area, info.CarFileCid, true)

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
