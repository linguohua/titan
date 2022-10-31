package scheduler

import (
	"sync"

	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/node/scheduler/db/cache"
	"github.com/linguohua/titan/node/scheduler/db/persistent"
	"golang.org/x/xerrors"
)

// DataManager Data
type DataManager struct {
	nodeManager   *NodeManager
	dataMap       sync.Map
	blockLoaderCh chan bool
	// dataTaskCh    chan bool
	// resultListLock *sync.Mutex
	// resultList     []*api.CacheResultInfo
}

func newDataManager(nodeManager *NodeManager) *DataManager {
	d := &DataManager{
		nodeManager:   nodeManager,
		blockLoaderCh: make(chan bool),
		// dataTaskCh:    make(chan bool),
		// resultList:     make([]*api.CacheResultInfo, 0),
		// resultListLock: &sync.Mutex{},
	}

	go d.startBlockLoader()
	// go d.startDataTask()

	return d
}

func (m *DataManager) findData(area, cid string) *Data {
	dataI, ok := m.dataMap.Load(cid)
	if !ok {
		data := loadData(area, cid, m.nodeManager, m)
		if data != nil {
			m.dataMap.Store(cid, data)
			return data
		}

	} else {
		return dataI.(*Data)
	}

	return nil
}

func (m *DataManager) cacheData(area, cid string, reliability int) error {
	isSave := false
	dataI, ok := m.dataMap.Load(cid)
	var data *Data
	if !ok {
		data = loadData(area, cid, m.nodeManager, m)
		if data == nil {
			isSave = true
			data = newData(area, m.nodeManager, m, cid, reliability)
		}

		m.dataMap.Store(cid, data)
	} else {
		data = dataI.(*Data)
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
	dataI, ok := m.dataMap.Load(cid)
	var data *Data
	if !ok {
		data = loadData(area, cid, m.nodeManager, m)
		if data == nil {
			return xerrors.Errorf("%s,cid:%s,cacheID:%s", ErrNotFoundTask, cid, cacheID)
		}

		m.dataMap.Store(cid, data)
	} else {
		data = dataI.(*Data)
	}

	return data.cacheContinue(cacheID)
}

func (m *DataManager) removeBlock(deviceID string, cids []string) {
	// TODO remove data info
	log.Errorf("removeBlock deviceID:%s,cids:%v", deviceID, cids)
}

func (m *DataManager) cacheCarfileResult(deviceID string, info *api.CacheResultInfo) error {
	area := m.nodeManager.getNodeArea(deviceID)
	data := m.findData(area, info.CarFileCid)

	if data == nil {
		return xerrors.Errorf("%s : %s", ErrNotFoundTask, info.CarFileCid)
	}

	return data.updateDataInfo(deviceID, info.CacheID, info)
}

// func (m *DataManager) dequeue() *api.CacheResultInfo {
// 	m.resultListLock.Lock()
// 	defer m.resultListLock.Unlock()

// 	info := m.resultList[0]
// 	m.resultList = m.resultList[1:]

// 	return info
// }

// func (m *DataManager) enqueue(info *api.CacheResultInfo) {
// 	m.resultListLock.Lock()
// 	defer m.resultListLock.Unlock()

// 	m.resultList = append(m.resultList, info)
// }

// func (m *DataManager) doDataTask() {
// 	info, err := cache.GetDB().GetWaitingCacheTask()
// 	if err != nil {
// 		log.Errorf("doDataTask GetWaitingCacheTask err:%s", err.Error())
// 		return
// 	}

// 	err = m.cacheData(serverArea, info.Cid, info.NeedReliability)
// 	if err != nil {
// 		log.Errorf("doDataTask cacheData err:%s", err.Error())
// 		return
// 	}

// 	err = cache.GetDB().RemoveWaitingCacheTask()
// 	if err != nil {
// 		log.Errorf("doDataTask RemoveWaitingCacheTask err:%s", err.Error())
// 		return
// 	}
// }

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

	// for len(m.resultList) > 0 {
	// 	info := m.dequeue()

	// 	err := m.cacheCarfileResult(info.DeviceID, info)
	// 	if err != nil {
	// 		log.Errorf("doResultTask cacheCarfileResult err:%s", err.Error())
	// 	}
	// }
}

// func (m *DataManager) pushNewTaskToQueue(area, cid string, reliability int) error {
// 	err := cache.GetDB().SetWaitingCacheTask(api.CacheDataInfo{Cid: cid, NeedReliability: reliability})

// 	// m.enqueue(info)
// 	m.notifyTask()

// 	return err
// }

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

// func (m *DataManager) startDataTask() {
// 	for {
// 		<-m.dataTaskCh
// 		m.doDataTask()
// 	}
// }

// func (m *DataManager) notifyTask() {
// 	select {
// 	case m.dataTaskCh <- true:
// 	default:
// 	}
// }
