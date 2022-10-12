package scheduler

import "github.com/linguohua/titan/api"

// DataManager Data
type DataManager struct {
	nodeManager *NodeManager
	dataMap     map[string]*Data
}

func newDataManager(nodeManager *NodeManager) *DataManager {
	return &DataManager{
		nodeManager: nodeManager,
		dataMap:     make(map[string]*Data),
	}
}

func (m *DataManager) cacheData(cid string, reliability int) {
	data, ok := m.dataMap[cid]
	if ok {
		return
	}

	data = newData(m.nodeManager, cid, reliability)
	data.createCache()
}

func (m *DataManager) cacheResult(deviceID string, info api.CacheResultInfo) {
	data := m.dataMap[deviceID] // TODO

	isFind := false
	for _, cache := range data.cacheMap {
		block, ok := cache.blockMap[info.Cid]
		if ok {
			block.isSuccess = info.IsOK
			// block.reliability = TODO
			isFind = true
			break
		}
	}

	if isFind {
		// data.reliability TODO
		// save db
	}
}

// Data Data
type Data struct {
	nodeManager     *NodeManager
	cid             string
	cacheMap        map[string]*Cache
	reliability     int
	needReliability int

	Total int
}

func newData(nodeManager *NodeManager, cid string, reliability int) *Data {
	return &Data{
		nodeManager:     nodeManager,
		cid:             cid,
		reliability:     0,
		needReliability: reliability,
		cacheMap:        make(map[string]*Cache),
	}
}

func (d *Data) createCache() error {
	c, err := newCache(d.nodeManager, d.cid)
	if err != nil {
		log.Errorf("new cache err:%v", err.Error())
		return err
	}

	d.cacheMap[c.id] = c
	d.saveData()

	return c.doCache(d.cid)
}

func (d *Data) saveData() {
	// TODO save to db
}
