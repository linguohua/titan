package scheduler

import (
	"context"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/node/scheduler/db/cache"
	"github.com/linguohua/titan/node/scheduler/db/persistent"
	"golang.org/x/xerrors"
)

// Cache Cache
type Cache struct {
	data        *Data
	nodeManager *NodeManager
	cacheID     string
	carfileCid  string
	status      persistent.CacheStatus
	reliability int
	doneSize    int
	doneBlocks  int
	totalSize   int
	totalBlocks int
	nodes       int
	isRootCache bool
	// removeBlocks int
}

func newCacheID(cid string) (string, error) {
	u2 := uuid.New()

	s := strings.Replace(u2.String(), "-", "", -1)
	return s, nil
}

func newCache(nodeManager *NodeManager, data *Data, cid string, isRootCache bool) (*Cache, error) {
	id, err := newCacheID(cid)
	if err != nil {
		return nil, err
	}

	cache := &Cache{
		nodeManager: nodeManager,
		data:        data,
		reliability: 0,
		status:      persistent.CacheStatusCreate,
		cacheID:     id,
		carfileCid:  cid,
		isRootCache: isRootCache,
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

	info, err := persistent.GetDB().GetCacheInfo(cacheID)
	// list, err := persistent.GetDB().GetCacheInfos(area, cacheID)
	if err != nil || info == nil {
		log.Errorf("loadCache %s,%s GetCacheInfo err:%v", carfileCid, cacheID, err)
		return nil
	}

	c.doneSize = info.DoneSize
	c.doneBlocks = info.DoneBlocks
	c.status = persistent.CacheStatus(info.Status)
	c.reliability = info.Reliability
	c.nodes = info.Nodes
	c.totalBlocks = info.TotalBlocks
	// c.removeBlocks = info.RemoveBlocks
	c.totalSize = info.TotalSize
	c.isRootCache = info.RootCache
	// info.ExpiredTime

	return c
}

func (c *Cache) cacheBlocksToNode(deviceID string, blocks []api.BlockInfo) (int64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cNode := c.nodeManager.getCandidateNode(deviceID)
	if cNode != nil {
		reqDatas := cNode.getReqCacheDatas(c.nodeManager, blocks, c.carfileCid, c.cacheID)

		nodeCacheStat, err := cNode.nodeAPI.CacheBlocks(ctx, reqDatas)
		if err != nil {
			log.Errorf("candidate %s, CacheData err:%s", deviceID, err.Error())
		} else {
			cNode.updateCacheStat(nodeCacheStat)
		}
		return cNode.nodeCacheNeedTime, err
	}

	eNode := c.nodeManager.getEdgeNode(deviceID)
	if eNode != nil {
		reqDatas := eNode.getReqCacheDatas(c.nodeManager, blocks, c.carfileCid, c.cacheID)

		nodeCacheStat, err := eNode.nodeAPI.CacheBlocks(ctx, reqDatas)
		if err != nil {
			log.Errorf("edge %s, CacheData err:%s", deviceID, err.Error())
		} else {
			eNode.updateCacheStat(nodeCacheStat)
		}

		return eNode.nodeCacheNeedTime, err
	}

	return 0, xerrors.Errorf("%s:%s", ErrNodeNotFind, deviceID)
}

func (c *Cache) findNode(filterDeviceIDs map[string]string, i int) (deviceID string) {
	deviceID = ""

	if c.isRootCache {
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

func (c *Cache) matchingNodeAndBlocks(cids map[string]string) ([]*persistent.BlockInfo, map[string][]api.BlockInfo) {
	if cids == nil {
		return nil, nil
	}
	// c.totalBlocks += len(cids)

	nodeCacheMap := make(map[string][]api.BlockInfo)
	blockList := make([]*persistent.BlockInfo, 0)

	i := 0
	for cid, dbID := range cids {
		i++
		status := persistent.CacheStatusFail
		deviceID := ""
		fid := 0

		nodes, err := persistent.GetDB().GetNodesWithCache(cid, false)
		if err != nil {
			log.Errorf("matchingNodeAndBlock cache:%s,cid:%s, GetNodesWithCacheList err:%s", c.cacheID, cid, err.Error())
		} else {
			filterDeviceIDs := make(map[string]string)
			if nodes != nil {
				for _, dID := range nodes {
					filterDeviceIDs[dID] = cid
				}
			}

			deviceID = c.findNode(filterDeviceIDs, i)
			if deviceID != "" {
				status = persistent.CacheStatusCreate

				cList, ok := nodeCacheMap[deviceID]
				if !ok {
					cList = make([]api.BlockInfo, 0)
				}

				fid, err = cache.GetDB().IncrNodeCacheFid(deviceID, 1)
				if err != nil {
					log.Errorf("deviceID:%s,IncrNodeCacheFid:%s", deviceID, err.Error())
					continue
				}

				cList = append(cList, api.BlockInfo{Cid: cid, Fid: fid})
				nodeCacheMap[deviceID] = cList
			}
		}

		b := &persistent.BlockInfo{
			CacheID:   c.cacheID,
			CID:       cid,
			DeviceID:  deviceID,
			Status:    int(status),
			Size:      0,
			ID:        dbID,
			CarfileID: c.carfileCid,
			// IsUpdate:  isUpdate,
			FID: fid,
		}

		blockList = append(blockList, b)
	}

	return blockList, nodeCacheMap
}

func (c *Cache) cacheDataToNodes(nodeCacheMap map[string][]api.BlockInfo) {
	if nodeCacheMap == nil {
		return
	}

	timeStamp := time.Now().Unix()
	timeStampMax := timeStamp

	for deviceID, caches := range nodeCacheMap {
		needTime, err := c.cacheBlocksToNode(deviceID, caches)
		if err != nil {
			log.Errorf("cacheBlocksToNode err:%s", err.Error())
			continue
		}

		t := timeStampMax + needTime
		if t > timeStampMax {
			timeStampMax = t
		}
	}

	// TODO update data/cache timeout
	c.data.dataManager.updateDataTimeout()

	return
}

func (c *Cache) blockCacheResult(info *api.CacheResultInfo) error {
	blockInfo, err := persistent.GetDB().GetBlockInfo(info.CacheID, info.Cid, info.DeviceID)
	if err != nil || blockInfo == nil {
		return xerrors.Errorf("blockCacheResult cacheID:%s,cid:%s,deviceID:%s, GetCacheInfo err:%v", info.CacheID, info.Cid, info.DeviceID, err)
	}

	if blockInfo.Status == int(persistent.CacheStatusSuccess) {
		return xerrors.Errorf("blockCacheResult cacheID:%s,%s block saved ", info.CacheID, info.Cid)
	}

	if info.Cid == c.carfileCid {
		c.totalSize = int(info.LinksSize) + info.BlockSize
		c.totalBlocks = 1
	}
	c.totalBlocks += len(info.Links)

	status := persistent.CacheStatusFail
	// fid := 0
	reliability := 0
	if info.IsOK {
		c.doneBlocks++
		c.doneSize += info.BlockSize
		status = persistent.CacheStatusSuccess
		// fid = info.Fid
		reliability = c.calculateReliability(blockInfo.DeviceID)
	}

	bInfo := &persistent.BlockInfo{
		ID:          blockInfo.ID,
		CacheID:     c.cacheID,
		CID:         blockInfo.CID,
		DeviceID:    blockInfo.DeviceID,
		Size:        info.BlockSize,
		Status:      int(status),
		Reliability: reliability,
		CarfileID:   c.carfileCid,
		// FID:         blockInfo.FID,
		// IsUpdate:    true,
	}

	linkMap := make(map[string]string)
	if len(info.Links) > 0 {
		for _, link := range info.Links {
			linkMap[link] = ""
		}
	}

	createBlocks, nodeCacheMap := c.matchingNodeAndBlocks(linkMap)

	// save info to db
	err = c.data.updateAndSaveCacheingInfo(bInfo, info, c, createBlocks)
	if err != nil {
		return xerrors.Errorf("blockCacheResult cacheID:%s,%s UpdateCacheInfo err:%s ", info.CacheID, info.Cid, err.Error())
	}

	if len(linkMap) == 0 {
		unDoneBlocks, err := persistent.GetDB().GetBloackCountWithStatus(c.cacheID, int(persistent.CacheStatusCreate))
		if err != nil {
			return xerrors.Errorf("blockCacheResult cacheID:%s,%s GetBloackCountWithStatus err:%s ", info.CacheID, info.Cid, err.Error())
		}

		if unDoneBlocks <= 0 {
			return c.endCache(unDoneBlocks)
		}
	}

	c.cacheDataToNodes(nodeCacheMap)

	return nil
}

func (c *Cache) startCache(cids map[string]string) error {
	createBlocks, nodeCacheMap := c.matchingNodeAndBlocks(cids)

	err := persistent.GetDB().SetBlockInfos(createBlocks, c.carfileCid)
	if err != nil {
		return xerrors.Errorf("startCache %s, SetBlockInfos err:%s", c.cacheID, err.Error())
	}

	if len(nodeCacheMap) <= 0 {
		return xerrors.Errorf("startCache %s fail not find node", c.cacheID)
	}

	// log.Infof("start cache %s,%s ---------- ", c.carfileCid, c.cacheID)
	c.cacheDataToNodes(nodeCacheMap)

	return nil
}

func (c *Cache) endCache(unDoneBlocks int) (err error) {
	// log.Infof("end cache %s,%s ----------", c.carfileCid, c.cacheID)

	defer func() {
		c.data.endData(c)
	}()

	failedBlocks, err := persistent.GetDB().GetBloackCountWithStatus(c.cacheID, int(persistent.CacheStatusFail))
	if err != nil {
		err = xerrors.Errorf("endCache %s,%s GetBloackCountWithStatus err:%v", c.carfileCid, c.cacheID, err.Error())
		return
	}

	if failedBlocks > 0 || unDoneBlocks > 0 {
		c.status = persistent.CacheStatusFail
	} else {
		c.status = persistent.CacheStatusSuccess
	}
	c.reliability = c.calculateReliability("")

	return
}

func (c *Cache) removeCache() error {
	reliability := c.data.reliability

	cidMap, err := persistent.GetDB().GetAllBlocks(c.cacheID)
	if err != nil {
		return err
	}

	for deviceID, cids := range cidMap {
		if deviceID == "" {
			continue
		}
		c.removeCacheBlocks(deviceID, cids)
	}

	reliability -= c.reliability

	c.data.cacheMap.Delete(c.cacheID)

	isDelete := true
	c.data.cacheMap.Range(func(key, value interface{}) bool {
		if value != nil {
			c := value.(*Cache)
			if c != nil {
				isDelete = false
			}
		}

		return true
	})

	// delete cache and update data info
	err = persistent.GetDB().RemoveAndUpdateCacheInfo(c.cacheID, c.carfileCid, isDelete, reliability)
	if err == nil {
		c.data.reliability = reliability
	}

	return err
}

func (c *Cache) removeCacheBlocks(deviceID string, cids []string) {
	ctx := context.Background()

	edge := c.nodeManager.getEdgeNode(deviceID)
	if edge != nil {
		_, err := edge.nodeAPI.DeleteBlocks(ctx, cids)
		if err != nil {
			log.Warnf("removeCacheBlocks DeleteBlocks err:%s", err.Error())
		}

		return
	}

	candidate := c.nodeManager.getCandidateNode(deviceID)
	if candidate != nil {
		_, err := candidate.nodeAPI.DeleteBlocks(ctx, cids)
		if err != nil {
			log.Warnf("removeCacheBlocks DeleteBlocks err:%s", err.Error())
		}

		return
	}
}

func (c *Cache) calculateReliability(deviceID string) int {
	if c.status == persistent.CacheStatusSuccess {
		return 1
	}

	return 0
}
