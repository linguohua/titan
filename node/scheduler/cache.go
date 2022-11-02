package scheduler

import (
	"context"
	"fmt"
	"strings"
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
	// area        string
	cacheID    string
	carFileCid string
	// blockMap    sync.Map
	status      cacheStatus
	reliability int
	doneSize    int
	doneBlocks  int
	dbID        int

	// unDoneBlocks   int
	// failedBlocks   int
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

func newCache(nodeManager *NodeManager, data *Data, cid string) (*Cache, error) {
	id, err := newCacheID(cid)
	if err != nil {
		return nil, err
	}

	cache := &Cache{
		// area:        area,
		nodeManager: nodeManager,
		data:        data,
		reliability: 0,
		status:      cacheStatusCreate,
		cacheID:     id,
		carFileCid:  cid,
	}

	return cache, err
}

func loadCache(cacheID, carfileCid string, nodeManager *NodeManager, data *Data) *Cache {
	if cacheID == "" {
		return nil
	}
	c := &Cache{
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

	// c.unDoneBlocks, err = persistent.GetDB().HaveBlocks(c.cacheID, int(cacheStatusCreate))
	// if err != nil {
	// 	log.Errorf("loadCache %s,%s HaveBlocks err:%v", carfileCid, cacheID, err)
	// }

	// c.failedBlocks, err = persistent.GetDB().HaveBlocks(c.cacheID, int(cacheStatusFail))
	// if err != nil {
	// 	log.Errorf("loadCache %s,%s HaveBlocks err:%v", carfileCid, cacheID, err)
	// }

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
		cs := c.nodeManager.findEdgeNodeWithGeo(nil, filterDeviceIDs)
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

	cs := c.nodeManager.findCandidateNodeWithGeo(nil, filterDeviceIDs)
	if cs == nil || len(cs) <= 0 {
		return
	}
	// rand node
	node := cs[i%len(cs)]

	deviceID = node.deviceInfo.DeviceId
	deviceAddr = node.addr
	return
}

func (c *Cache) getCacheBlockInfos(cids map[string]int, isHaveCache bool) ([]*persistent.BlockInfo, map[string][]string) {
	if cids == nil {
		return nil, nil
	}
	// c.unDoneBlocks += len(cids)

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

	err = cache.GetDB().SetRunningCacheTask(c.carFileCid, c.cacheID)
	if err != nil {
		return err
	}

	status := cacheStatusFail
	fid := ""
	if info.IsOK {
		c.doneBlocks++
		c.doneSize += info.BlockSize
		status = cacheStatusSuccess
		fid = info.Fid
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

	createBlocks, deviceCacheMap := c.getCacheBlockInfos(linkMap, c.data.haveRootCache())

	// save info to db
	err = c.data.updateDataInfo(bInfo, fid, info, c, createBlocks)
	if err != nil {
		return err
	}

	unDoneBlocks, err := persistent.GetDB().HaveBlocks(c.cacheID, int(cacheStatusCreate))
	if err != nil {
		log.Errorf("loadCache %s,%s HaveBlocks err:%v", c.carFileCid, c.cacheID, err)
		return err
	}

	if unDoneBlocks <= 0 {
		c.endCache(unDoneBlocks)
		return nil
	}

	c.cacheToNodes(deviceCacheMap)
	return nil
}

func (c *Cache) startCache(cids map[string]int, haveRootCache bool) error {
	createBlocks, deviceCacheMap := c.getCacheBlockInfos(cids, haveRootCache)

	c.saveBlockInfos(createBlocks)
	if len(deviceCacheMap) <= 0 {
		// log.Infof("%s cache fail not find node ---------- ", c.cacheID)
		return xerrors.Errorf("cache %s fail not find node", c.cacheID)
	}

	err := cache.GetDB().SetRunningCacheTask(c.carFileCid, c.cacheID)
	if err != nil {
		return err
	}

	err = cache.GetDB().SetCidToRunningList(c.carFileCid, c.cacheID)
	if err != nil {
		return err
	}

	log.Infof("%s cache start ---------- ", c.cacheID)
	c.cacheToNodes(deviceCacheMap)

	return nil
}

func (c *Cache) endCache(unDoneBlocks int) {
	cache.GetDB().RemoveRunningCacheTask(c.carFileCid, c.cacheID)
	log.Infof("%s cache end ---------- ", c.cacheID)

	failedBlocks, err := persistent.GetDB().HaveBlocks(c.cacheID, int(cacheStatusFail))
	if err != nil {
		log.Errorf("loadCache %s,%s HaveBlocks err:%v", c.carFileCid, c.cacheID, err)
		return
	}

	if failedBlocks > 0 || unDoneBlocks > 0 {
		c.reliability = 0
		c.status = cacheStatusFail
	} else {
		c.reliability = 1
		c.status = cacheStatusSuccess
	}

	c.data.endData(c)
}

func (c *Cache) removeCache() error {
	reliability := c.data.reliability

	cidMap, err := persistent.GetDB().GetAllBlocks(c.cacheID)
	if err != nil {
		return err
	}

	haveErr := false

	for deviceID, cids := range cidMap {
		if deviceID == "" {
			continue
		}
		errorMap, err := c.removeBlocks(deviceID, cids)
		if err != nil {
			log.Errorf("removeBlocks err:%s", err.Error())
			haveErr = true
		}
		if len(errorMap) > 0 {
			log.Errorf("removeBlocks errorMap:%v", errorMap)
			haveErr = true
		}
	}

	if !haveErr {
		newCacheList := make([]string, 0)
		cacheIDs := ""
		rootCacheID := c.data.rootCacheID
		reliability -= c.reliability
		idList := strings.Split(c.data.cacheIDs, ",")
		for _, cacheID := range idList {
			if cacheID != "" && cacheID != c.cacheID {
				newCacheList = append(newCacheList, cacheID)
				cacheIDs = fmt.Sprintf("%s%s,", cacheIDs, cacheID)
			}
		}
		if c.cacheID == rootCacheID {
			rootCacheID = ""
		}
		// delete cache and update data info
		err = persistent.GetDB().RemoveCacheInfo(c.cacheID, c.carFileCid, cacheIDs, rootCacheID, newCacheList, reliability)
	}

	return err
}

func (c *Cache) removeBlocks(deviceID string, cids []string) (map[string]string, error) {
	ctx := context.Background()
	errorMap := make(map[string]string)

	nodeFinded := false

	edge := c.nodeManager.getEdgeNode(deviceID)
	if edge != nil {
		results, err := edge.nodeAPI.DeleteBlocks(ctx, cids)
		if err != nil {
			return nil, err
		}

		nodeFinded = true

		if len(results) > 0 {
			for _, data := range results {
				errorMap[data.Cid] = fmt.Sprintf("node err:%s", data.ErrMsg)
			}
		}

	}

	candidate := c.nodeManager.getCandidateNode(deviceID)
	if candidate != nil {
		resultList, err := candidate.nodeAPI.DeleteBlocks(ctx, cids)
		if err != nil {
			return nil, err
		}

		nodeFinded = true

		if len(resultList) > 0 {
			for _, data := range resultList {
				errorMap[data.Cid] = fmt.Sprintf("node err:%s", data.ErrMsg)
			}
		}

	}

	if !nodeFinded {
		return nil, xerrors.Errorf("%s:%s", ErrNodeNotFind, deviceID)
	}

	delRecordList := make([]string, 0)
	for _, cid := range cids {
		if errorMap[cid] != "" {
			continue
		}

		delRecordList = append(delRecordList, cid)
	}

	err := persistent.GetDB().DeleteBlockInfos(c.carFileCid, c.cacheID, deviceID, delRecordList)
	if err != nil {
		return errorMap, err
	}

	return errorMap, err
}
