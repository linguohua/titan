package scheduler

import (
	"strings"
	"sync"

	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/node/scheduler/db/persistent"
)

// DataManager Data
type DataManager struct {
	nodeManager *NodeManager
	dataMap     map[string]*Data

	nodeCacheTask sync.Map
}

// CacheTask Cache Task
type CacheTask struct {
	carfileID string
	cacheID   string
}

func newDataManager(nodeManager *NodeManager) *DataManager {
	return &DataManager{
		nodeManager: nodeManager,
		dataMap:     make(map[string]*Data),
	}
}

func (m *DataManager) cacheData(cid string, reliability int) error {
	data, ok := m.dataMap[cid]
	if !ok {
		data = newData(m.nodeManager, m, cid, reliability)
		m.dataMap[cid] = data

		// load db data
		dInfo, _ := persistent.GetDB().GetDataInfo(cid)
		if dInfo != nil {
			data.cacheIDs = dInfo.CacheIDs
			data.totalSize = dInfo.TotalSize

			idList := strings.Split(dInfo.CacheIDs, ",")
			for _, cacheID := range idList {
				if cacheID == "" {
					continue
				}
				c := &Cache{
					cacheID:     cacheID,
					cardFileCid: cid,
					nodeManager: m.nodeManager,
					blockMap:    make(map[string]*BlockInfo),
				}

				list, err := persistent.GetDB().GetCacheInfos(cacheID)
				if err == nil && list != nil {
					for _, cInfo := range list {
						c.blockMap[cInfo.CID] = &BlockInfo{
							cid:       cInfo.CID,
							deviceID:  cInfo.DeviceID,
							isSuccess: cInfo.Status == 1,
							size:      cInfo.TotalSize,
						}

						c.doneSize += cInfo.TotalSize
					}
				}

				if c.doneSize >= data.totalSize {
					c.status = 1
				}

				data.cacheMap[cacheID] = c
			}
		}

		// return xerrors.New("already exists")
	}

	return data.createCache(m)
}

func (m *DataManager) cacheResult(deviceID string, info api.CacheResultInfo) {
	task := m.getCacheTask(deviceID)
	if task == nil {
		return
	}

	data, ok := m.dataMap[task.carfileID]
	if ok {
		data.cacheResult(deviceID, task.cacheID, info)
	}
}

func (m *DataManager) addCacheTaskMap(deviceID, cid, cacheID string) {
	cacheTask := &CacheTask{carfileID: cid, cacheID: cacheID}
	m.nodeCacheTask.Store(deviceID, cacheTask)
}

func (m *DataManager) removeCacheTaskMap(deviceID string) {
	m.nodeCacheTask.Delete(deviceID)
}

func (m *DataManager) getCacheTask(deviceID string) *CacheTask {
	val, ok := m.nodeCacheTask.Load(deviceID)
	if ok {
		return val.(*CacheTask)
	}

	return nil
}
