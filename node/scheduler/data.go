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
		totalBlocks:     1,
	}
}

func loadDataInfos(area string) []*persistent.DataInfo {
	infos, err := persistent.GetDB().GetDataInfos()
	if err != nil {
		return nil
	}

	return infos
}

func loadData(area, cid string, nodeManager *NodeManager, dataManager *DataManager) *Data {
	dInfo, err := persistent.GetDB().GetDataInfo(cid)
	if err != nil {
		log.Errorf("loadData %s err :%s", cid, err.Error())
		return nil
	}
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

func (d *Data) cacheContinue(cacheID string) error {
	cacheI, ok := d.cacheMap.Load(cacheID)
	if !ok || cacheI == nil {
		return xerrors.Errorf("Not Found CacheID :%s", cacheID)
	}
	cache := cacheI.(*Cache)

	list, err := persistent.GetDB().GetUndoneBlocks(cacheID)
	if err != nil {
		return err
	}

	log.Infof("%s cache continue ---------- ", cache.cacheID)
	// return cache.doCache(list, d.haveRootCache())

	createBlocks, deviceCacheMap := cache.getCacheInfos(list, d.haveRootCache())

	cache.saveBlockInfos(createBlocks)

	cache.cacheToNodes(deviceCacheMap)

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

func (d *Data) createCache() (*Cache, error) {
	if d.reliability >= d.needReliability {
		return nil, xerrors.Errorf("reliability is enough:%d/%d", d.reliability, d.needReliability)
	}

	cache, err := newCache(d.area, d.nodeManager, d, d.cid)
	if err != nil {
		return nil, xerrors.Errorf("new cache err:%s", err.Error())
	}

	return cache, nil
}

func (d *Data) updateDataInfo(deviceID, cacheID string, info *api.CacheResultInfo) error {
	cacheI, ok := d.cacheMap.Load(cacheID)
	if !ok {
		return xerrors.Errorf("updateDataInfo not found cacheID:%s,Cid:%s", cacheID, d.cid)
	}
	cache := cacheI.(*Cache)

	blockInfo, links, err := cache.blockCacheResult(info)
	if err != nil {
		return xerrors.Errorf("updateBlockInfo err:%s", err.Error())
	}

	if !d.haveRootCache() {
		if info.Cid == d.cid {
			d.totalSize = int(info.LinksSize) + info.BlockSize
		}
		d.totalBlocks += len(info.Links)
		// isUpdate = true
	}

	if cache.status > cacheStatusCreate {
		d.cacheTime++

		if cache.status == cacheStatusSuccess {
			d.reliability += cache.reliability
			if d.rootCacheID == "" {
				d.rootCacheID = cacheID
			}
		}

		d.saveCacheTotalInfo(cache, blockInfo, info.Fid, nil)

		d.endData()
		// isUpdate = true
		return nil
	}

	// if isUpdate {
	// 	d.saveData()
	// }

	createBlocks, deviceCacheMap := cache.getCacheInfos(links, d.haveRootCache())

	d.saveCacheTotalInfo(cache, blockInfo, info.Fid, createBlocks)

	cache.cacheToNodes(deviceCacheMap)

	return nil
}

func (d *Data) saveCacheTotalInfo(cache *Cache, bInfo *persistent.BlockInfo, fid string, createBlocks []*persistent.BlockInfo) {
	dInfo := &persistent.DataInfo{
		CID:         d.cid,
		TotalSize:   d.totalSize,
		TotalBlocks: d.totalBlocks,
		Reliability: d.reliability,
		CacheTime:   d.cacheTime,
		RootCacheID: d.rootCacheID,
	}

	cInfo := &persistent.CacheInfo{
		ID:          cache.dbID,
		CarfileID:   cache.cardFileCid,
		CacheID:     cache.cacheID,
		DoneSize:    cache.doneSize,
		Status:      int(cache.status),
		DoneBlocks:  cache.doneBlocks,
		Reliability: cache.reliability,
	}

	err := persistent.GetDB().SaveCacheResult(dInfo, cInfo, bInfo, fid, createBlocks)
	if err != nil {
		log.Errorf("cid:%s,SaveCacheResult err:%s", d.cid, err.Error())
	}
}

func (d *Data) startData() error {
	if d.running {
		return xerrors.New("data have task running,please wait")
	}

	cache, err := d.createCache()
	if err != nil {
		return err
	}

	// d.cacheMap[cache.cacheID] = cache
	d.cacheMap.Store(cache.cacheID, cache)
	d.cacheIDs = fmt.Sprintf("%s%s,", d.cacheIDs, cache.cacheID)

	id, err := persistent.GetDB().CreateCache(
		&persistent.DataInfo{CID: d.cid, CacheIDs: d.cacheIDs},
		&persistent.CacheInfo{
			CarfileID: cache.cardFileCid,
			CacheID:   cache.cacheID,
			Status:    int(cache.status),
		})
	if err != nil {
		return err
	}
	cache.dbID = id

	return cache.startCache(d.haveRootCache())
}

func (d *Data) endData() {
	d.running = false

	if d.needReliability <= d.reliability {
		return
	}

	if d.cacheTime > d.needReliability+1 {
		// TODO
		return
	}

	// create cache again
	err := d.startData()
	if err != nil {
		log.Errorf("startData err:%s", err.Error())
	}
}
