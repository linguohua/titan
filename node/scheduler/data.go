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
	running         bool
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
	cacheI, ok := d.cacheMap.Load(cacheID)
	if !ok || cacheI == nil {
		return xerrors.Errorf("Not Found CacheID :%s", cacheID)
	}
	cache := cacheI.(*Cache)

	list, err := persistent.GetDB().GetUndoneCaches(serverArea, cacheID)
	if err != nil {
		return err
	}
	// cache.blockMap.Range(func(key, value interface{}) bool {
	// 	block := value.(*Block)
	// 	if block.status != cacheStatusSuccess {
	// 		list = append(list, block.cid)
	// 	}

	// 	return true
	// })

	// log.Infof("do cache list : %s", list)

	log.Infof("%s cache continue ---------- ", cache.cacheID)
	return cache.doCache(list, d.haveRootCache())
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
	if d.running {
		return xerrors.New("data have task running,please wait")
	}

	if d.reliability >= d.needReliability {
		return xerrors.Errorf("reliability is enough:%d/%d", d.reliability, d.needReliability)
	}

	cache, err := newCache(d.area, d.nodeManager, d, d.cid)
	if err != nil {
		return xerrors.Errorf("new cache err:%s", err.Error())
	}

	// d.cacheMap[cache.cacheID] = cache
	d.cacheMap.Store(cache.cacheID, cache)
	d.cacheIDs = fmt.Sprintf("%s,%s", d.cacheIDs, cache.cacheID)

	log.Infof("%s cache start ---------- ", cache.cacheID)
	return cache.doCache(map[string]int{d.cid: 0}, d.haveRootCache())
}

func (d *Data) updateDataInfo(deviceID, cacheID string, info *api.CacheResultInfo) error {
	cacheI, ok := d.cacheMap.Load(cacheID)
	if !ok {
		return xerrors.Errorf("updateDataInfo not found cacheID:%s,Cid:%s", cacheID, d.cid)
	}
	cache := cacheI.(*Cache)

	isUpdate := false
	if d.haveRootCache() && info.Cid == d.cid {
		d.totalSize = int(info.LinksSize) + info.BlockSize

		isUpdate = true
	}

	links := cache.updateBlockInfo(info, d.totalSize, d.reliability)

	if cache.status > cacheStatusCreate {
		d.cacheTime++

		if cache.status == cacheStatusSuccess {
			d.reliability += cache.reliability
			if d.rootCacheID == "" {
				d.rootCacheID = cacheID
				d.running = false
			}
		}

		if d.needReliability > d.reliability {
			if d.cacheTime < d.needReliability+1 { // TODO
				// create cache again
				d.createCache(d.dataManager)
			}
		}
		isUpdate = true
	}

	if isUpdate {
		d.saveData()
	}

	if links != nil {
		return cache.doCache(links, d.haveRootCache())
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
		log.Errorf("cid:%s,SetDataInfo err:%s", d.cid, err.Error())
	}
}
