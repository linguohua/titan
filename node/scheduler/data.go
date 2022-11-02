package scheduler

import (
	"fmt"
	"strings"
	"sync"

	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/node/scheduler/db/cache"
	"github.com/linguohua/titan/node/scheduler/db/persistent"
	"golang.org/x/xerrors"
)

// Data Data
type Data struct {
	nodeManager *NodeManager
	dataManager *DataManager
	// area            string
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

func newData(nodeManager *NodeManager, dataManager *DataManager, cid string, reliability int) *Data {
	return &Data{
		nodeManager:     nodeManager,
		dataManager:     dataManager,
		cid:             cid,
		reliability:     0,
		needReliability: reliability,
		cacheTime:       0,
		// area:            area,
		totalBlocks: 1,
		rootCacheID: "",
	}
}

func loadData(cid string, nodeManager *NodeManager, dataManager *DataManager) *Data {
	dInfo, err := persistent.GetDB().GetDataInfo(cid)
	if err != nil {
		log.Errorf("loadData %s err :%s", cid, err.Error())
		return nil
	}
	if dInfo != nil {
		data := newData(nodeManager, dataManager, cid, 0)
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
			c := loadCache(cacheID, cid, nodeManager, data)
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
	cID, _ := cache.GetDB().GetRunningCacheTask(d.cid)
	if cID != "" {
		return xerrors.New("data have task running,please wait")
	}

	cacheI, ok := d.cacheMap.Load(cacheID)
	if !ok || cacheI == nil {
		return xerrors.Errorf("Not Found CacheID :%s", cacheID)
	}
	cache := cacheI.(*Cache)

	list, err := persistent.GetDB().GetUndoneBlocks(cacheID)
	if err != nil {
		return err
	}
	// return cache.doCache(list, d.haveRootCache())

	return cache.startCache(list, d.haveRootCache())
}

func (d *Data) haveRootCache() bool {
	// log.Infof("%v d.rootCacheID ---------- ", d.rootCacheID)
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

	cache, err := newCache(d.nodeManager, d, d.cid)
	if err != nil {
		return nil, xerrors.Errorf("new cache err:%s", err.Error())
	}

	return cache, nil
}

func (d *Data) updateDataInfo(blockInfo *persistent.BlockInfo, fid string, info *api.CacheResultInfo, c *Cache, createBlocks []*persistent.BlockInfo) error {
	if !d.haveRootCache() {
		if info.Cid == d.cid {
			d.totalSize = int(info.LinksSize) + info.BlockSize
		}
		d.totalBlocks += len(info.Links)
		// isUpdate = true
	}

	return d.saveCacheingResults(c, blockInfo, info.Fid, createBlocks)
}

func (d *Data) saveCacheingResults(cache *Cache, bInfo *persistent.BlockInfo, fid string, createBlocks []*persistent.BlockInfo) error {
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
		CarfileID:   cache.carFileCid,
		CacheID:     cache.cacheID,
		DoneSize:    cache.doneSize,
		Status:      int(cache.status),
		DoneBlocks:  cache.doneBlocks,
		Reliability: cache.reliability,
	}

	err := persistent.GetDB().SaveCacheingResults(dInfo, cInfo, bInfo, fid, createBlocks)
	if err != nil {
		log.Errorf("cid:%s,SaveCacheingResults err:%s", d.cid, err.Error())
	}

	return err
}

func (d *Data) saveCacheEndResults(cache *Cache) error {
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
		CarfileID:   cache.carFileCid,
		CacheID:     cache.cacheID,
		DoneSize:    cache.doneSize,
		Status:      int(cache.status),
		DoneBlocks:  cache.doneBlocks,
		Reliability: cache.reliability,
	}

	err := persistent.GetDB().SaveCacheEndResults(dInfo, cInfo)
	if err != nil {
		log.Errorf("cid:%s,SaveCacheEndResults err:%s", d.cid, err.Error())
	}

	return err
}

func (d *Data) startData() error {
	if d.needReliability <= d.reliability {
		return xerrors.Errorf("reliability is enough:%d/%d", d.reliability, d.needReliability)
	}

	cacheID, _ := cache.GetDB().GetRunningCacheTask(d.cid)
	if cacheID != "" {
		return xerrors.New("data have task running,please wait")
	}

	c, err := d.createCache()
	if err != nil {
		return err
	}

	d.cacheMap.Store(c.cacheID, c)
	d.cacheIDs = fmt.Sprintf("%s%s,", d.cacheIDs, c.cacheID)

	id, err := persistent.GetDB().CreateCache(
		&persistent.DataInfo{CID: d.cid, CacheIDs: d.cacheIDs},
		&persistent.CacheInfo{
			CarfileID: c.carFileCid,
			CacheID:   c.cacheID,
			Status:    int(c.status),
		})
	if err != nil {
		return err
	}
	c.dbID = id

	return c.startCache(map[string]int{c.carFileCid: 0}, d.haveRootCache())
}

func (d *Data) endData(c *Cache) {
	var err error
	d.cacheTime++

	if c.status == cacheStatusSuccess {
		d.reliability += c.reliability
		if !d.haveRootCache() {
			d.rootCacheID = c.cacheID
		}
	}

	defer func() {
		if err != nil {
			d.dataManager.dataTaskEnd(d.cid)
		}
	}()

	err = d.saveCacheEndResults(c)
	if err != nil {
		log.Errorf("saveCacheEndResults err:%s", err.Error())
		return
	}

	if d.cacheTime > d.needReliability {
		err = xerrors.New("cacheTime greater than needReliability")
		return
	}

	// create cache again
	err = d.startData()
	if err != nil {
		log.Errorf("startData err:%s", err.Error())
	}
}
