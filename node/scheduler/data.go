package scheduler

import (
	"fmt"
	"strings"
	"sync"

	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/node/scheduler/db/persistent"
	"golang.org/x/xerrors"
)

// Data Data
type Data struct {
	nodeManager     *NodeManager
	dataManager     *DataManager
	area            string
	cid             string
	cacheMap        sync.Map
	cacheIDs        string
	reliability     int
	needReliability int
	totalSize       int
	cacheTime       int
	rootCacheID     string
	totalBlocks     int
}

func newData(area string, nodeManager *NodeManager, dataManager *DataManager, cid string, reliability int) *Data {
	return &Data{
		nodeManager:     nodeManager,
		dataManager:     dataManager,
		cid:             cid,
		reliability:     0,
		needReliability: reliability,
		cacheTime:       0,
		area:            area,
	}
}

func loadDataInfos(area string) []*persistent.DataInfo {
	infos, err := persistent.GetDB().GetDataInfos(area)
	if err != nil {
		return nil
	}

	return infos
}

func loadData(area, cid string, nodeManager *NodeManager, dataManager *DataManager) *Data {
	dInfo, _ := persistent.GetDB().GetDataInfo(area, cid)
	if dInfo != nil {
		data := newData(area, nodeManager, dataManager, cid, 0)
		data.cacheIDs = dInfo.CacheIDs
		data.totalSize = dInfo.TotalSize
		data.needReliability = dInfo.NeedReliability
		data.reliability = dInfo.Reliability
		data.cacheTime = dInfo.CacheTime
		data.rootCacheID = dInfo.RootCacheID
		data.totalBlocks = dInfo.TotalBlocks

		idList := strings.Split(dInfo.CacheIDs, ",")
		for _, cacheID := range idList {
			if cacheID == "" {
				continue
			}
			c := loadCache(area, cacheID, cid, nodeManager, data.totalSize)
			if c == nil {
				continue
			}

			// data.cacheMap[cacheID] = c
			data.cacheMap.Store(cacheID, c)
		}

		return data
	}

	return nil
}

func (d *Data) cacheContinue(dataManager *DataManager, cacheID string) error {
	// cache := d.cacheMap[cacheID]
	cacheI, ok := d.cacheMap.Load(cacheID)
	if !ok || cacheI == nil {
		return xerrors.Errorf("Not Found CacheID :%s", cacheID)
	}
	cache := cacheI.(*Cache)

	list := make([]string, 0)

	cache.blockMap.Range(func(key, value interface{}) bool {
		block := value.(*Block)
		if block.status != cacheStatusSuccess {
			list = append(list, block.cid)
		}

		return true
	})

	// log.Infof("do cache list : %s", list)

	log.Infof("%s cache continue ---------- ", cache.cacheID)
	cache.doCache(list, d.haveRootCache())

	return nil
}

func (d *Data) haveRootCache() bool {
	if d.rootCacheID == "" {
		return false
	}

	cI, ok := d.cacheMap.Load(d.rootCacheID)
	if ok {
		cache := cI.(*Cache)
		return cache.status == cacheStatusSuccess
	}

	return false
}

func (d *Data) createCache(dataManager *DataManager) error {
	cache, err := newCache(d.area, d.nodeManager, dataManager, d.cid)
	if err != nil {
		log.Errorf("new cache err:%v", err.Error())
		return err
	}

	// d.cacheMap[cache.cacheID] = cache
	d.cacheMap.Store(cache.cacheID, cache)
	d.cacheIDs = fmt.Sprintf("%s,%s", d.cacheIDs, cache.cacheID)

	log.Infof("%s cache start ---------- ", cache.cacheID)
	cache.doCache([]string{d.cid}, d.haveRootCache())

	return nil
}

func (d *Data) updateDataInfo(deviceID, cacheID string, info *api.CacheResultInfo) error {
	cacheI, ok := d.cacheMap.Load(cacheID)
	if !ok {
		return xerrors.Errorf("updateDataInfo not found cacheID:%v,Cid:%v", cacheID, d.cid)
	}
	cache := cacheI.(*Cache)

	isUpdate := false
	if d.reliability == 0 && info.Cid == d.cid {
		d.totalSize = int(info.LinksSize) + info.BlockSize

		isUpdate = true
	}

	links := cache.updateCacheInfo(info, d.totalSize, d.reliability)
	if links != nil {
		cache.doCache(links, d.haveRootCache())
	}

	if cache.status > cacheStatusCreate {
		d.cacheTime++
		d.reliability += cache.reliability

		if d.needReliability > d.reliability {
			if d.cacheTime < d.needReliability+2 { // TODO
				// create cache again
				d.createCache(d.dataManager)
			}
		}
		isUpdate = true
	}

	if isUpdate {
		d.saveData()
	}

	return nil
}

func (d *Data) saveData() {
	// save to db
	err := persistent.GetDB().SetDataInfo(d.area, &persistent.DataInfo{
		CID:             d.cid,
		CacheIDs:        d.cacheIDs,
		TotalSize:       d.totalSize,
		NeedReliability: d.needReliability,
		Reliability:     d.reliability,
		CacheTime:       d.cacheTime,
		TotalBlocks:     d.totalBlocks,
		RootCacheID:     d.rootCacheID,
	})
	if err != nil {
		log.Errorf("cid:%s,SetDataInfo err:%v", d.cid, err.Error())
	}
}
