package scheduler

import (
	"fmt"

	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/node/scheduler/db/persistent"
)

// Data Data
type Data struct {
	nodeManager *NodeManager
	dataManager *DataManager

	cid             string
	cacheMap        map[string]*Cache
	cacheIDs        string
	reliability     int
	needReliability int
	totalSize       int
}

func newData(nodeManager *NodeManager, dataManager *DataManager, cid string, reliability int) *Data {
	return &Data{
		nodeManager:     nodeManager,
		dataManager:     dataManager,
		cid:             cid,
		reliability:     0,
		needReliability: reliability,
		cacheMap:        make(map[string]*Cache),
	}
}

func (d *Data) createCache(dataManager *DataManager) error {
	c, err := newCache(d.nodeManager, dataManager, d.cid)
	if err != nil {
		log.Errorf("new cache err:%v", err.Error())
		return err
	}

	d.cacheMap[c.cacheID] = c
	d.cacheIDs = fmt.Sprintf("%s,%s", d.cacheIDs, c.cacheID)
	d.saveData()

	return c.doCache([]string{d.cid})
}

func (d *Data) cacheResult(deviceID, cacheID string, info api.CacheResultInfo) {
	// log.Warnf("cacheResult-----------------info.Links:%v,cacheID:%v", info.Links, cacheID)
	cache, ok := d.cacheMap[cacheID]
	if !ok {
		return
	}

	if info.Cid == d.cid {
		d.totalSize = int(info.LinksSize)
	} else {
		cache.doneSize += info.BlockSize
	}

	if cache.doneSize >= d.totalSize {
		cache.status = 1

		d.dataManager.removeCacheTaskMap(deviceID)
	}

	block, ok := cache.blockMap[info.Cid]
	if ok {
		block.isSuccess = info.IsOK
		block.size = info.BlockSize
		// block.reliability = TODO

		cache.saveCache(block, true)
		d.saveData()

		// continue cache
		if len(info.Links) > 0 {
			cache.doCache(info.Links)
		}
	}
}

func (d *Data) saveData() {
	// save to db
	persistent.GetDB().SetDataInfo(&persistent.DataInfo{
		CID:       d.cid,
		CacheIDs:  d.cacheIDs,
		TotalSize: d.totalSize,
	})
}
