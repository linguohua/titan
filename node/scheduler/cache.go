package scheduler

import (
	"context"
	"fmt"
	"strings"

	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/node/scheduler/db/cache"
	"github.com/linguohua/titan/node/scheduler/db/persistent"
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
	cacheID     string
	carfileCid  string
	status      cacheStatus
	reliability int
	doneSize    int
	doneBlocks  int
	// dbID        int

	// timewheelCache *timewheel.TimeWheel
	// lastUpdateTime time.Time
}

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
		nodeManager: nodeManager,
		data:        data,
		reliability: 0,
		status:      cacheStatusCreate,
		cacheID:     id,
		carfileCid:  cid,
	}

	return cache, err
}

func loadCache(cacheID, carfileCid string, nodeManager *NodeManager, data *Data) *Cache {
	if cacheID == "" {
		return nil
	}
	c := &Cache{
		cacheID:     cacheID,
		carfileCid:  carfileCid,
		nodeManager: nodeManager,
		data:        data,
	}

	info, err := persistent.GetDB().GetCacheInfo(cacheID, carfileCid)
	// list, err := persistent.GetDB().GetCacheInfos(area, cacheID)
	if err != nil || info == nil {
		log.Errorf("loadCache %s,%s GetCacheInfo err:%v", carfileCid, cacheID, err)
		return nil
	}

	// c.dbID = info.ID
	c.doneSize = info.DoneSize
	c.doneBlocks = info.DoneBlocks
	c.status = cacheStatus(info.Status)
	c.reliability = info.Reliability

	return c
}

func (c *Cache) cacheBlocksToNode(deviceID string, cids []string) error {
	cNode := c.nodeManager.getCandidateNode(deviceID)
	if cNode != nil {
		reqDatas := cNode.getReqCacheDatas(c.nodeManager, cids, c.carfileCid, c.cacheID)

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
		reqDatas := eNode.getReqCacheDatas(c.nodeManager, cids, c.carfileCid, c.cacheID)

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

func (c *Cache) findNode(isHaveCache bool, filterDeviceIDs map[string]string, i int) (deviceID string) {
	deviceID = ""

	if isHaveCache {
		cs := c.nodeManager.findEdgeNodes(nil, filterDeviceIDs)
		if cs == nil || len(cs) <= 0 {
			return
		}
		// rand node
		// node := cs[randomNum(0, len(cs))]
		node := cs[i%len(cs)]

		deviceID = node.deviceInfo.DeviceId
		// deviceAddr = node.addr
		return
	}

	cs := c.nodeManager.findCandidateNodes(nil, filterDeviceIDs)
	if cs == nil || len(cs) <= 0 {
		return
	}
	// rand node
	node := cs[i%len(cs)]

	deviceID = node.deviceInfo.DeviceId
	// deviceAddr = node.addr
	return
}

func (c *Cache) getCacheBlockInfos(cids map[string]int, isHaveCache bool) ([]*persistent.BlockInfo, map[string][]string) {
	if cids == nil {
		return nil, nil
	}
	// c.unDoneBlocks += len(cids)

	nodeCacheMap := make(map[string][]string)
	blockList := make([]*persistent.BlockInfo, 0)

	for cid, dbID := range cids {
		status := cacheStatusFail
		deviceID := ""

		ds, err := persistent.GetDB().GetNodesWithCacheList(cid)
		if err != nil {
			log.Errorf("cache:%s,cid:%s, GetNodesWithCacheList err:%s", c.cacheID, cid, err.Error())
		} else {
			filterDeviceIDs := make(map[string]string)
			if ds != nil {
				for _, d := range ds {
					filterDeviceIDs[d] = cid
				}
			}

			deviceID = c.findNode(isHaveCache, filterDeviceIDs, dbID)
			if deviceID != "" {
				status = cacheStatusCreate

				cList, ok := nodeCacheMap[deviceID]
				if !ok {
					cList = make([]string, 0)
				}

				cList = append(cList, cid)
				nodeCacheMap[deviceID] = cList
			}
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

	return blockList, nodeCacheMap
}

func (c *Cache) cacheToNodes(nodeCacheMap map[string][]string) {
	if nodeCacheMap == nil {
		return
	}

	for deviceID, caches := range nodeCacheMap {
		err := c.cacheBlocksToNode(deviceID, caches)
		if err != nil {
			log.Errorf("cacheBlocks err:%s", err.Error())
			continue
		}
	}
}

func (c *Cache) blockCacheResult(info *api.CacheResultInfo) error {
	cacheInfo, err := persistent.GetDB().GetBlockInfo(info.CacheID, info.Cid, info.DeviceID)
	if err != nil || cacheInfo == nil {
		return xerrors.Errorf("cacheID:%s,cid:%s,deviceID:%s, GetCacheInfo err:%v", info.CacheID, info.Cid, info.DeviceID, err)
	}

	if cacheInfo.Status == int(cacheStatusSuccess) {
		return xerrors.Errorf("cacheID:%s,%s block saved ", info.CacheID, info.Cid)
	}

	err = cache.GetDB().SetRunningCacheTask(c.carfileCid, c.cacheID)
	if err != nil {
		return xerrors.Errorf("cacheID:%s,%s SetRunningCacheTask err:%s ", info.CacheID, info.Cid, err.Error())
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

	createBlocks, nodeCacheMap := c.getCacheBlockInfos(linkMap, c.data.haveRootCache())

	// save info to db
	err = c.data.updateDataInfo(bInfo, fid, info, c, createBlocks)
	if err != nil {
		return xerrors.Errorf("cacheID:%s,%s updateDataInfo err:%s ", info.CacheID, info.Cid, err.Error())
	}

	unDoneBlocks, err := persistent.GetDB().HaveBlocks(c.cacheID, int(cacheStatusCreate))
	if err != nil {
		return xerrors.Errorf("cacheID:%s,%s HaveBlocks err:%s ", info.CacheID, info.Cid, err.Error())
	}

	if unDoneBlocks <= 0 {
		return c.endCache(unDoneBlocks)
	}

	c.cacheToNodes(nodeCacheMap)
	return nil
}

func (c *Cache) startCache(cids map[string]int, haveRootCache bool) error {
	createBlocks, nodeCacheMap := c.getCacheBlockInfos(cids, haveRootCache)

	err := persistent.GetDB().SetBlockInfos(createBlocks)
	if err != nil {
		return xerrors.Errorf("startCache %s, SetBlockInfos err:%s", c.cacheID, err.Error())
	}

	if len(nodeCacheMap) <= 0 {
		return xerrors.Errorf("startCache %s fail not find node", c.cacheID)
	}

	err = cache.GetDB().SetRunningCacheTask(c.carfileCid, c.cacheID)
	if err != nil {
		return xerrors.Errorf("startCache %s , SetRunningCacheTask err:%s", c.cacheID, err.Error())
	}

	err = cache.GetDB().SetCidToRunningList(c.carfileCid, c.cacheID)
	if err != nil {
		return xerrors.Errorf("startCache %s , SetCidToRunningList err:%s", c.cacheID, err.Error())
	}

	log.Infof("start cache %s,%s ---------- ", c.carfileCid, c.cacheID)
	c.cacheToNodes(nodeCacheMap)

	return nil
}

func (c *Cache) endCache(unDoneBlocks int) (err error) {
	log.Infof("end cache %s,%s ----------", c.carfileCid, c.cacheID)

	defer func() {
		err = c.data.endData(c)
		return
	}()

	err = cache.GetDB().RemoveRunningCacheTask(c.carfileCid, c.cacheID)
	if err != nil {
		err = xerrors.Errorf("endCache RemoveRunningCacheTask err: %s", err.Error())
		return
	}

	failedBlocks, err := persistent.GetDB().HaveBlocks(c.cacheID, int(cacheStatusFail))
	if err != nil {
		err = xerrors.Errorf("endCache %s,%s HaveBlocks err:%v", c.carfileCid, c.cacheID, err.Error())
		return
	}

	if failedBlocks > 0 || unDoneBlocks > 0 {
		c.reliability = 0
		c.status = cacheStatusFail
	} else {
		c.reliability = 1
		c.status = cacheStatusSuccess
	}

	return
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
			log.Errorf("%s, removeBlocks err:%s", deviceID, err.Error())
			haveErr = true
		}
		if len(errorMap) > 0 {
			log.Errorf("%s,removeBlocks errorMap:%v", deviceID, errorMap)
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
		err = persistent.GetDB().RemoveCacheInfo(c.cacheID, c.carfileCid, cacheIDs, rootCacheID, newCacheList, reliability)
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

	err := persistent.GetDB().DeleteBlockInfos(c.carfileCid, c.cacheID, deviceID, delRecordList)
	if err != nil {
		return errorMap, err
	}

	return errorMap, err
}
