package scheduler

import (
	"fmt"
	"strings"

	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/node/scheduler/db/persistent"
	"golang.org/x/xerrors"
)

// Data Data
type Data struct {
	nodeManager *NodeManager
	dataManager *DataManager

	area            string
	cid             string
	cacheMap        map[string]*Cache
	cacheIDs        string
	reliability     int
	needReliability int
	totalSize       int
	cacheTime       int
}

func newData(area string, nodeManager *NodeManager, dataManager *DataManager, cid string, reliability int) *Data {
	return &Data{
		nodeManager:     nodeManager,
		dataManager:     dataManager,
		cid:             cid,
		reliability:     0,
		needReliability: reliability,
		cacheMap:        make(map[string]*Cache),
		cacheTime:       0,
		area:            area,
	}
}

// func loadDatas(){

// }

func loadData(area, cid string, nodeManager *NodeManager, dataManager *DataManager) *Data {
	dInfo, _ := persistent.GetDB().GetDataInfo(area, cid)
	if dInfo != nil {
		data := newData(area, nodeManager, dataManager, cid, 0)
		data.cacheIDs = dInfo.CacheIDs
		data.totalSize = dInfo.TotalSize
		data.needReliability = dInfo.NeedReliability
		data.reliability = dInfo.Reliability
		data.cacheTime = dInfo.CacheTime

		idList := strings.Split(dInfo.CacheIDs, ",")
		for _, cacheID := range idList {
			if cacheID == "" {
				continue
			}
			c := loadCache(area, cacheID, cid, nodeManager, data.totalSize)
			if c == nil {
				continue
			}

			data.cacheMap[cacheID] = c
		}

		return data
	}

	return nil
}

func (d *Data) cacheContinue(dataManager *DataManager, cacheID string) error {
	cache := d.cacheMap[cacheID]
	if cache == nil {
		return xerrors.New(ErrCidNotFind)
	}

	list := make([]string, 0)
	for _, block := range cache.blockMap {
		if block.status != cacheStatusSuccess {
			list = append(list, block.cid)
		}
	}

	return cache.doCache(list, d.reliability > 0)
}

func (d *Data) createCache(dataManager *DataManager) error {
	// have old cache undone
	for _, cache := range d.cacheMap {
		if cache.status == cacheStatusFail {
			list := make([]string, 0)
			for _, block := range cache.blockMap {
				if block.status == cacheStatusFail {
					list = append(list, block.cid)
				}
			}

			return cache.doCache(list, d.reliability > 0)
		}
	}

	cache, err := newCache(d.area, d.nodeManager, dataManager, d.cid)
	if err != nil {
		log.Errorf("new cache err:%v", err.Error())
		return err
	}

	d.cacheMap[cache.cacheID] = cache
	d.cacheIDs = fmt.Sprintf("%s,%s", d.cacheIDs, cache.cacheID)

	return cache.doCache([]string{d.cid}, d.reliability > 0)
}

func (d *Data) updateDataInfo(deviceID, cacheID string, info *api.CacheResultInfo) (string, string) {
	// log.Warnf("cacheResult-----------------info.Links:%v,cacheID:%v", info.Links, cacheID)
	cache, ok := d.cacheMap[cacheID]
	if !ok {
		log.Errorf("updateDataInfo not found cacheID:%v,Cid:%v", cacheID, d.cid)
		return d.cid, ""
	}

	isUpdate := false
	if d.reliability == 0 && info.Cid == d.cid {
		d.totalSize = int(info.LinksSize) + info.BlockSize

		isUpdate = true
	}

	cache.updateCacheInfo(info, d.totalSize, d.reliability)

	if cache.status > cacheStatusCreate {
		d.cacheTime++
		d.dataManager.removeCacheTask(deviceID)
		d.reliability += cache.reliability
		// log.Warnf("--------- reliability:%v,%v", d.reliability, cache.reliability)

		if d.needReliability > d.reliability {
			if d.cacheTime < d.needReliability+2 { // TODO
				// create cache again
				d.createCache(d.dataManager)
			}
			// log.Errorf("need create cache needReliability:%v, reliability:%v, status:%v", d.needReliability, d.reliability, cache.status)
		}
		isUpdate = true
	}

	if isUpdate {
		d.saveData()
	}

	return d.cid, cacheID
}

func (d *Data) saveData() {
	// save to db
	persistent.GetDB().SetDataInfo(d.area, &persistent.DataInfo{
		CID:             d.cid,
		CacheIDs:        d.cacheIDs,
		TotalSize:       d.totalSize,
		NeedReliability: d.needReliability,
		Reliability:     d.reliability,
		CacheTime:       d.cacheTime,
	})
}
