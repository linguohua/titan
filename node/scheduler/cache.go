package scheduler

import (
	"context"
	"fmt"
	"time"

	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/node/scheduler/db/cache"
	"github.com/linguohua/titan/node/scheduler/db/persistent"
	"github.com/ouqiang/timewheel"
	"golang.org/x/xerrors"
)

type cacheStatus int

const (
	cacheStatusUnknown cacheStatus = iota
	cacheStatusCreate
	cacheStatusFail
	cacheStatusSuccess
)

// Cache Cache
type Cache struct {
	data        *Data
	nodeManager *NodeManager
	area        string
	cacheID     string
	carFileCid  string
	// blockMap    sync.Map
	status      cacheStatus
	reliability int
	doneSize    int
	doneBlocks  int
	dbID        int

	unDoneBlocks   int
	failedBlocks   int
	timewheelCache *timewheel.TimeWheel
	lastUpdateTime time.Time
}

// Block Block Info
// type Block struct {
// 	cid         string
// 	deviceID    string
// 	deviceArea  string
// 	deviceIP    string
// 	status      cacheStatus
// 	reliability int
// 	size        int
// }

func newCacheID(cid string) (string, error) {
	fid, err := cache.GetDB().IncrCacheID(serverArea)
	if err != nil {
		return "", err
	}

	aName := persistent.GetDB().ReplaceArea()

	return fmt.Sprintf("%s_cache_%d", aName, fid), nil
}

func newCache(area string, nodeManager *NodeManager, data *Data, cid string) (*Cache, error) {
	id, err := newCacheID(cid)
	if err != nil {
		return nil, err
	}

	cache := &Cache{
		area:        area,
		nodeManager: nodeManager,
		data:        data,
		reliability: 0,
		status:      cacheStatusCreate,
		cacheID:     id,
		carFileCid:  cid,
	}

	return cache, err
}

func loadCache(area, cacheID, carfileCid string, nodeManager *NodeManager, data *Data) *Cache {
	if cacheID == "" {
		return nil
	}
	c := &Cache{
		area:        area,
		cacheID:     cacheID,
		carFileCid:  carfileCid,
		nodeManager: nodeManager,
		data:        data,
	}

	info, err := persistent.GetDB().GetCacheInfo(cacheID, carfileCid)
	// list, err := persistent.GetDB().GetCacheInfos(area, cacheID)
	if err != nil || info == nil {
		log.Errorf("loadCache %s,%s GetCacheInfo err:%v", carfileCid, cacheID, err)
		return nil
	}

	c.dbID = info.ID
	c.doneSize = info.DoneSize
	c.doneBlocks = info.DoneBlocks
	c.status = cacheStatus(info.Status)
	c.reliability = info.Reliability // TODO

	c.unDoneBlocks, err = persistent.GetDB().HaveBlocks(c.cacheID, int(cacheStatusCreate))
	if err != nil {
		log.Errorf("loadCache %s,%s HaveBlocks err:%v", carfileCid, cacheID, err)
	}

	c.failedBlocks, err = persistent.GetDB().HaveBlocks(c.cacheID, int(cacheStatusFail))
	if err != nil {
		log.Errorf("loadCache %s,%s HaveBlocks err:%v", carfileCid, cacheID, err)
	}

	return c
}

// func (c *Cache) startTimer() {
// 	if c.timewheelCache != nil {
// 		return
// 	}
// 	tt := 1 // (minute)
// 	// timewheel
// 	c.timewheelCache = timewheel.New(time.Second, 3600, func(_ interface{}) {
// 		if c.status > cacheStatusCreate {
// 			c.timewheelCache.RemoveTimer("cache")
// 			c.timewheelCache = nil
// 			return
// 		}

// 		nowTime := time.Now().Add(-time.Duration(tt*60) * time.Second)
// 		if !c.lastUpdateTime.After(nowTime) {
// 			c.timewheelCache.RemoveTimer("cache")
// 			c.timewheelCache = nil
// 			// timeout
// 			c.endCache()
// 			return
// 		}
// 		c.timewheelCache.AddTimer((time.Duration(tt)*60-1)*time.Second, "cache", nil)
// 	})
// 	c.timewheelCache.Start()
// 	c.timewheelCache.AddTimer(time.Duration(tt)*60*time.Second, "cache", nil)
// }

func (c *Cache) cacheBlocksToNode(deviceID string, cids []string) error {
	cNode := c.nodeManager.getCandidateNode(deviceID)
	if cNode != nil {
		reqDatas := cNode.getReqCacheDatas(c.nodeManager, cids, c.carFileCid, c.cacheID)

		for _, reqData := range reqDatas {
			err := cNode.nodeAPI.CacheBlocks(context.Background(), reqData)
			if err != nil {
				log.Errorf("candidate CacheData err:%s,url:%s,cids:%v", err.Error(), reqData.CandidateURL, reqData.BlockInfos)
			}
		}
		return nil
	}

	eNode := c.nodeManager.getEdgeNode(deviceID)
	if eNode != nil {
		reqDatas := eNode.getReqCacheDatas(c.nodeManager, cids, c.carFileCid, c.cacheID)

		for _, reqData := range reqDatas {
			err := eNode.nodeAPI.CacheBlocks(context.Background(), reqData)
			if err != nil {
				log.Errorf("edge CacheData err:%s,url:%s,cids:%v", err.Error(), reqData.CandidateURL, reqData.BlockInfos)
			}
		}
		return nil
	}

	return xerrors.Errorf("%s:%s", ErrNodeNotFind, deviceID)
}

func (c *Cache) findNode(isHaveCache bool, filterDeviceIDs map[string]string, i int) (deviceID, deviceAddr string) {
	deviceID = ""
	deviceAddr = ""

	if isHaveCache {
		cs := c.nodeManager.findEdgeNodeWithGeo(c.area, nil, filterDeviceIDs)
		if cs == nil || len(cs) <= 0 {
			return
		}
		// rand node
		// node := cs[randomNum(0, len(cs))]
		node := cs[i%len(cs)]

		deviceID = node.deviceInfo.DeviceId
		deviceAddr = node.addr
		return
	}

	cs := c.nodeManager.findCandidateNodeWithGeo(c.area, nil, filterDeviceIDs)
	if cs == nil || len(cs) <= 0 {
		return
	}
	// rand node
	node := cs[i%len(cs)]

	deviceID = node.deviceInfo.DeviceId
	deviceAddr = node.addr
	return
}

func (c *Cache) getCacheInfos(cids map[string]int, isHaveCache bool) ([]*persistent.BlockInfo, map[string][]string) {
	if cids == nil {
		return nil, nil
	}
	c.unDoneBlocks += len(cids)

	deviceCacheMap := make(map[string][]string)
	blockList := make([]*persistent.BlockInfo, 0)

	for cid, dbID := range cids {
		filterDeviceIDs := make(map[string]string)
		ds, err := persistent.GetDB().GetNodesWithCacheList(cid)
		if err != nil {
			log.Errorf("cache:%s, GetNodesWithCacheList err:%s", c.cacheID, err.Error())
		}
		if ds != nil {
			for _, d := range ds {
				filterDeviceIDs[d] = cid
			}
		}

		status := cacheStatusFail

		deviceID, _ := c.findNode(isHaveCache, filterDeviceIDs, dbID)
		if deviceID != "" {
			status = cacheStatusCreate

			cList, ok := deviceCacheMap[deviceID]
			if !ok {
				cList = make([]string, 0)
			}

			cList = append(cList, cid)
			deviceCacheMap[deviceID] = cList
		}

		b := &persistent.BlockInfo{
			CacheID:  c.cacheID,
			CID:      cid,
			DeviceID: deviceID,
			Status:   int(status),
			Size:     0,
			ID:       dbID,
		}

		blockList = append(blockList, b)
	}

	return blockList, deviceCacheMap
}

func (c *Cache) cacheToNodes(deviceCacheMap map[string][]string) {
	if deviceCacheMap == nil {
		return
	}

	for deviceID, caches := range deviceCacheMap {
		err := c.cacheBlocksToNode(deviceID, caches)
		if err != nil {
			log.Errorf("cacheBlocks err:%s", err.Error())
			continue
		}
	}
}

func (c *Cache) saveBlockInfos(blocks []*persistent.BlockInfo) {
	err := persistent.GetDB().SetBlockInfos(blocks)
	if err != nil {
		log.Errorf("cacheID:%s,SetCacheInfos err:%v", c.cacheID, err.Error())
	}
}

func (c *Cache) blockCacheResult(info *api.CacheResultInfo) error {
	cacheInfo, err := persistent.GetDB().GetBlockInfo(info.CacheID, info.Cid, info.DeviceID)
	if err != nil || cacheInfo == nil {
		return xerrors.Errorf("CacheID:%s,cid:%s,deviceID:%s, updateBlockInfo GetCacheInfo err:%v", info.CacheID, info.Cid, info.DeviceID, err)
	}

	if cacheInfo.Status == int(cacheStatusSuccess) {
		return xerrors.Errorf("%s block saved ", info.Cid)
	}

	// c.startTimer()

	c.unDoneBlocks--

	status := cacheStatusFail
	fid := ""
	if info.IsOK {
		c.doneBlocks++
		c.doneSize += info.BlockSize
		status = cacheStatusSuccess
		fid = info.Fid
	} else {
		c.failedBlocks++
	}

	bInfo := &persistent.BlockInfo{
		ID:          cacheInfo.ID,
		CacheID:     c.cacheID,
		CID:         cacheInfo.CID,
		DeviceID:    cacheInfo.DeviceID,
		Size:        info.BlockSize,
		Status:      int(status),
		Reliability: 1,
	}

	linkMap := make(map[string]int)
	if len(info.Links) > 0 {
		for _, link := range info.Links {
			linkMap[link] = 0
		}
	}

	createBlocks, deviceCacheMap := c.getCacheInfos(linkMap, c.data.haveRootCache())

	if c.unDoneBlocks <= 0 {
		c.endCache()
	}

	// save info to db
	err = c.data.updateDataInfo(bInfo, fid, info, c, createBlocks)
	if err != nil {
		return err
	}

	c.cacheToNodes(deviceCacheMap)
	return nil
}

func (c *Cache) startCache(cids map[string]int, haveRootCache bool) error {
	cache.GetDB().SetRunningCacheTask(c.carFileCid)
	log.Infof("%s cache start ---------- ", c.cacheID)
	// c.startTimer()

	createBlocks, deviceCacheMap := c.getCacheInfos(cids, haveRootCache)

	c.saveBlockInfos(createBlocks)
	if len(deviceCacheMap) <= 0 {
		// log.Infof("%s cache fail not find node ---------- ", c.cacheID)
		return xerrors.Errorf("cache %s fail not find node", c.cacheID)
	}

	c.cacheToNodes(deviceCacheMap)

	return nil
}

func (c *Cache) endCache() {
	cache.GetDB().RemoveRunningCacheTask(c.carFileCid)
	log.Infof("%s cache end ---------- ", c.cacheID)

	if c.failedBlocks > 0 || c.unDoneBlocks > 0 {
		c.reliability = 0
		c.status = cacheStatusFail
	} else {
		c.reliability = 1
		c.status = cacheStatusSuccess
	}
	c.data.endData(c)
}

func (c *Cache) removeBlocks() error {
	// cidMap, err := persistent.GetDB().GetAllBlocks(c.cacheID)
	// if err != nil {
	// 	return err
	// }

	// for deviceID, cids := range cidMap {
	// 	node := c.nodeManager.getCandidateNode(deviceID)
	// 	if node != nil {
	// 		errList, err := node.nodeAPI.DeleteBlocks(context.Background(), cids)
	// 		if err != nil {
	// 			log.Errorf("%s DeleteBlocks err:%s", deviceID, err.Error())
	// 			continue
	// 		}

	// 	}
	// }

	return nil
}
