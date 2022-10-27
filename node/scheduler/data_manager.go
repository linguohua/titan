package scheduler

import (
	"sync"

	"github.com/linguohua/titan/api"
	"golang.org/x/xerrors"
)

// DataManager Data
type DataManager struct {
	nodeManager    *NodeManager
	dataMap        sync.Map
	blockLoaderCh  chan bool
	resultListLock *sync.Mutex
	resultList     []*api.CacheResultInfo
}

func newDataManager(nodeManager *NodeManager) *DataManager {
	d := &DataManager{
		nodeManager:    nodeManager,
		blockLoaderCh:  make(chan bool),
		resultList:     make([]*api.CacheResultInfo, 0),
		resultListLock: &sync.Mutex{},
	}

	go d.startBlockLoader()

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
	dataI, ok := m.dataMap.Load(cid)
	var data *Data
	if !ok {
		data = loadData(area, cid, m.nodeManager, m)
		if data == nil {
			data = newData(area, m.nodeManager, m, cid, reliability)
		}

		m.dataMap.Store(cid, data)
	} else {
		data = dataI.(*Data)
	}

	data.needReliability = reliability

	defer data.saveData()

	return data.createCache(m)
}

func (m *DataManager) cacheContinue(area, cid, cacheID string) error {
	dataI, ok := m.dataMap.Load(cid)
	var data *Data
	if !ok {
		data = loadData(area, cid, m.nodeManager, m)
		if data == nil {
			return xerrors.Errorf("%s,cid:%s,cacheID:%v", ErrNotFoundTask, cid, cacheID)
		}

		m.dataMap.Store(cid, data)
	} else {
		data = dataI.(*Data)
	}

	return data.cacheContinue(m, cacheID)
}

func (m *DataManager) removeBlock(deviceID string, cids []string) {
	// TODO remove data info
	log.Errorf("removeBlock deviceID:%v,cids:%v", deviceID, cids)
}

func (m *DataManager) cacheCarfileResult(deviceID string, info *api.CacheResultInfo) error {
	area := m.nodeManager.getNodeArea(deviceID)
	data := m.findData(area, info.CarFileCid)

	if data == nil {
		return xerrors.Errorf("%s : %s", ErrNotFoundTask, info.CarFileCid)
	}

	return data.updateDataInfo(deviceID, info.CacheID, info)
}

func (m *DataManager) dequeue() *api.CacheResultInfo {
	m.resultListLock.Lock()
	defer m.resultListLock.Unlock()

	info := m.resultList[0]
	m.resultList = m.resultList[1:]

	return info
}

func (m *DataManager) enqueue(info *api.CacheResultInfo) {
	m.resultListLock.Lock()
	defer m.resultListLock.Unlock()

	m.resultList = append(m.resultList, info)
}

func (m *DataManager) doResultTask() {
	for len(m.resultList) > 0 {
		info := m.dequeue()

		err := m.cacheCarfileResult(info.DeviceID, info)
		if err != nil {
			log.Errorf("doResultTask cacheCarfileResult err:%s", err.Error())
		}
	}
}

func (m *DataManager) pushResultToQueue(deviceID string, info *api.CacheResultInfo) {
	info.DeviceID = deviceID
	m.enqueue(info)
	m.notifyBlockLoader()
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
