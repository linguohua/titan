package scheduler

import (
	"context"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/node/helper"
	"github.com/linguohua/titan/node/scheduler/db/cache"
	"github.com/linguohua/titan/node/scheduler/db/persistent"
	"golang.org/x/xerrors"
)

// Cache Cache
type Cache struct {
	data        *Data
	nodeManager *NodeManager
	cacheID     string
	carfileHash string
	status      persistent.CacheStatus
	reliability int
	doneSize    int
	doneBlocks  int
	totalSize   int
	totalBlocks int
	nodes       int
	isRootCache bool
	expiredTime time.Time
}

func newCacheID() (string, error) {
	u2 := uuid.New()

	s := strings.Replace(u2.String(), "-", "", -1)
	return s, nil
}

func newCache(nodeManager *NodeManager, data *Data, hash string, isRootCache bool) (*Cache, error) {
	id, err := newCacheID()
	if err != nil {
		return nil, err
	}

	cache := &Cache{
		nodeManager: nodeManager,
		data:        data,
		reliability: 0,
		status:      persistent.CacheStatusCreate,
		cacheID:     id,
		carfileHash: hash,
		isRootCache: isRootCache,
		expiredTime: data.expiredTime,
	}

	err = persistent.GetDB().CreateCache(
		&persistent.CacheInfo{
			CarfileHash: cache.carfileHash,
			CacheID:     cache.cacheID,
			Status:      int(cache.status),
			ExpiredTime: cache.expiredTime,
			RootCache:   cache.isRootCache,
		})
	if err != nil {
		return nil, err
	}

	return cache, err
}

func loadCache(cacheID, carfileHash string, nodeManager *NodeManager, data *Data) *Cache {
	if cacheID == "" {
		return nil
	}
	c := &Cache{
		cacheID:     cacheID,
		carfileHash: carfileHash,
		nodeManager: nodeManager,
		data:        data,
	}

	info, err := persistent.GetDB().GetCacheInfo(cacheID)
	if err != nil || info == nil {
		log.Errorf("loadCache %s,%s GetCacheInfo err:%v", carfileHash, cacheID, err)
		return nil
	}

	c.doneSize = info.DoneSize
	c.doneBlocks = info.DoneBlocks
	c.status = persistent.CacheStatus(info.Status)
	c.reliability = info.Reliability
	c.nodes = info.Nodes
	c.totalBlocks = info.TotalBlocks
	c.totalSize = info.TotalSize
	c.isRootCache = info.RootCache
	c.expiredTime = info.ExpiredTime

	return c
}

// Notify node to cache blocks
func (c *Cache) cacheBlocksToNode(deviceID string, blocks []api.BlockInfo) (int64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cNode := c.nodeManager.getCandidateNode(deviceID)
	if cNode != nil {
		reqDatas := cNode.findDownloadinfoForBlocks(c.nodeManager, blocks, c.carfileHash, c.cacheID)

		nodeCacheStat, err := cNode.nodeAPI.CacheBlocks(ctx, reqDatas)
		if err != nil {
			log.Errorf("CacheBlocks %s, CacheBlocks err:%s", deviceID, err.Error())
		} else {
			cNode.updateCacheStat(nodeCacheStat)
		}
		return cNode.cacheNextTimeoutTimeStamp, err
	}

	eNode := c.nodeManager.getEdgeNode(deviceID)
	if eNode != nil {
		reqDatas := eNode.findDownloadinfoForBlocks(c.nodeManager, blocks, c.carfileHash, c.cacheID)

		nodeCacheStat, err := eNode.nodeAPI.CacheBlocks(ctx, reqDatas)
		if err != nil {
			log.Errorf("CacheBlocks %s, CacheBlocks err:%s", deviceID, err.Error())
		} else {
			eNode.updateCacheStat(nodeCacheStat)
		}

		return eNode.cacheNextTimeoutTimeStamp, err
	}

	return 0, xerrors.Errorf("%s:%s", ErrNodeNotFind, deviceID)
}

func (c *Cache) findIdleNode(skips map[string]string, i int) (deviceID string) {
	deviceID = ""

	if c.isRootCache {
		list := c.nodeManager.findCandidateNodes(nil, skips)
		if list == nil || len(list) <= 0 {
			return
		}

		sort.Slice(list, func(i, j int) bool {
			return list[i].cacheTimeoutTimeStamp < list[j].cacheTimeoutTimeStamp
		})

		// rand node
		node := list[i%len(list)]

		deviceID = node.deviceInfo.DeviceId
		return
	}

	list := c.nodeManager.findEdgeNodes(nil, skips)
	if list == nil || len(list) <= 0 {
		return
	}

	sort.Slice(list, func(i, j int) bool {
		return list[i].cacheTimeoutTimeStamp < list[j].cacheTimeoutTimeStamp
	})

	// rand node
	node := list[i%len(list)]

	deviceID = node.deviceInfo.DeviceId
	return
}

// Allocate blocks to nodes
func (c *Cache) allocateBlocksToNodes(cids map[string]string) ([]*persistent.BlockInfo, map[string][]api.BlockInfo) {
	if cids == nil {
		return nil, nil
	}

	nodeCacheMap := make(map[string][]api.BlockInfo)
	blockList := make([]*persistent.BlockInfo, 0)

	i := 0
	for cid, dbID := range cids {
		i++
		status := persistent.CacheStatusFail
		deviceID := ""
		from := "IPFS"
		fid := 0

		hash, err := helper.CIDString2HashString(cid)
		if err != nil {
			log.Errorf("allocateBlocksToNodes %s cid to hash err:%s", cid, err.Error())
			continue
		}

		froms, err := persistent.GetDB().GetNodesWithCache(hash, false)
		if err != nil {
			log.Errorf("allocateBlocksToNodes cache:%s,hash:%s, GetNodesWithCache err:%s", c.cacheID, hash, err.Error())
		} else {
			skips := make(map[string]string)
			if froms != nil {
				for _, dID := range froms {
					skips[dID] = cid

					// fid from
					node := c.nodeManager.getCandidateNode(dID)
					if node != nil {
						from = node.deviceInfo.DeviceId
					}
				}
			}

			deviceID = c.findIdleNode(skips, i)
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

				cList = append(cList, api.BlockInfo{Cid: cid, Fid: fid, From: from})
				nodeCacheMap[deviceID] = cList
			}
		}

		b := &persistent.BlockInfo{
			CacheID:     c.cacheID,
			CID:         cid,
			DeviceID:    deviceID,
			Status:      int(status),
			Size:        0,
			ID:          dbID,
			CarfileHash: c.carfileHash,
			FID:         fid,
			Source:      from,
			CIDHash:     hash,
		}

		blockList = append(blockList, b)
	}

	return blockList, nodeCacheMap
}

// Notify nodes to cache blocks and setting timeout
func (c *Cache) cacheBlocksToNodes(nodeCacheMap map[string][]api.BlockInfo) {
	if nodeCacheMap == nil || len(nodeCacheMap) <= 0 {
		return
	}

	// timeStamp := time.Now().Unix()
	needTimeMax := int64(0)

	for deviceID, caches := range nodeCacheMap {
		needTime, err := c.cacheBlocksToNode(deviceID, caches)
		if err != nil {
			log.Errorf("cacheBlocksToNodes deviceID:%s, err:%s", deviceID, err.Error())
			continue
		}

		if needTime > needTimeMax {
			needTimeMax = needTime
		}
	}

	timeStamp := time.Now().Unix()
	timeout := needTimeMax - timeStamp
	// update data task timeout
	c.data.dataManager.updateDataTimeout(c.carfileHash, c.cacheID, timeout)

	return
}

func (c *Cache) blockCacheResult(info *api.CacheResultInfo) error {
	hash, err := helper.CIDString2HashString(info.Cid)
	if err != nil {
		return xerrors.Errorf("blockCacheResult %s cid to hash err:%s", info.Cid, err.Error())
	}

	blockInfo, err := persistent.GetDB().GetBlockInfo(info.CacheID, hash, info.DeviceID)
	if err != nil || blockInfo == nil {
		return xerrors.Errorf("blockCacheResult cacheID:%s,hash:%s,deviceID:%s, GetBlockInfo err:%v", info.CacheID, hash, info.DeviceID, err)
	}

	if blockInfo.Status == int(persistent.CacheStatusSuccess) {
		return xerrors.Errorf("blockCacheResult cacheID:%s,%s block saved ", info.CacheID, info.Cid)
	}

	if info.Cid == c.data.carfileCid {
		c.totalSize = int(info.LinksSize) + info.BlockSize
		c.totalBlocks = 1
	}
	c.totalBlocks += len(info.Links)

	status := persistent.CacheStatusFail
	reliability := 0
	if info.IsOK {
		c.doneBlocks++
		c.doneSize += info.BlockSize
		status = persistent.CacheStatusSuccess
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
		CarfileHash: c.carfileHash,
	}

	linkMap := make(map[string]string)
	if len(info.Links) > 0 {
		for _, link := range info.Links {
			linkMap[link] = ""
		}
	}

	createBlocks, nodeCacheMap := c.allocateBlocksToNodes(linkMap)

	// save info to db
	err = c.data.updateAndSaveCacheingInfo(bInfo, c, createBlocks)
	if err != nil {
		return xerrors.Errorf("blockCacheResult cacheID:%s,%s updateAndSaveCacheingInfo err:%s ", info.CacheID, info.Cid, err.Error())
	}

	if len(linkMap) == 0 {
		unDoneBlocks, err := persistent.GetDB().GetBloackCountWithStatus(c.cacheID, int(persistent.CacheStatusCreate))
		if err != nil {
			return xerrors.Errorf("blockCacheResult cacheID:%s,%s GetBloackCountWithStatus err:%s ", info.CacheID, info.Cid, err.Error())
		}

		if unDoneBlocks <= 0 {
			return c.endCache(unDoneBlocks, false)
		}
	}

	c.cacheBlocksToNodes(nodeCacheMap)

	return nil
}

func (c *Cache) startCache(cids map[string]string) error {
	createBlocks, nodeCacheMap := c.allocateBlocksToNodes(cids)

	err := persistent.GetDB().SaveCacheingResults(nil, nil, nil, createBlocks)
	if err != nil {
		return xerrors.Errorf("startCache %s, SetBlockInfos err:%s", c.cacheID, err.Error())
	}

	if len(nodeCacheMap) <= 0 {
		return xerrors.Errorf("startCache %s fail not find node", c.cacheID)
	}

	// log.Infof("start cache %s,%s ---------- ", c.data.carfileCid, c.cacheID)
	c.cacheBlocksToNodes(nodeCacheMap)

	c.data.dataManager.saveEvent(c.data.carfileCid, c.cacheID, "", "", eventTypeDoCacheTaskStart)

	err = cache.GetDB().SetDataTaskToRunningList(c.carfileHash, c.cacheID)
	if err != nil {
		return xerrors.Errorf("startCache %s , SetDataTaskToRunningList err:%s", c.carfileHash, err.Error())
	}

	return nil
}

func (c *Cache) endCache(unDoneBlocks int, isTimeout bool) (err error) {
	// log.Infof("end cache %s,%s ----------", c.data.carfileCid, c.cacheID)
	msg := ""
	if isTimeout {
		msg = "timeout"
	}
	c.data.dataManager.saveEvent(c.data.carfileCid, c.cacheID, "", msg, eventTypeDoCacheTaskEnd)

	err = cache.GetDB().RemoveRunningDataTask(c.carfileHash, c.cacheID)
	if err != nil {
		err = xerrors.Errorf("stopCache RemoveRunningDataTask err: %s", err.Error())
		return
	}

	defer func() {
		// save message info
		c.setCacheMessageInfo()

		c.data.cacheEnd(c)
	}()

	if isTimeout {
		c.status = persistent.CacheStatusTimeout
		return
	}

	failedBlocks, err := persistent.GetDB().GetBloackCountWithStatus(c.cacheID, int(persistent.CacheStatusFail))
	if err != nil {
		err = xerrors.Errorf("stopCache %s,%s GetBloackCountWithStatus err:%v", c.data.carfileCid, c.cacheID, err.Error())
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

	blocks, err := persistent.GetDB().GetAllBlocks(c.cacheID)
	if err != nil {
		return err
	}

	cidMap := make(map[string][]string, 0)

	for _, block := range blocks {
		cids, ok := cidMap[block.DeviceID]
		if !ok {
			cids = make([]string, 0)
		}
		cids = append(cids, block.CID)
		cidMap[block.DeviceID] = cids
	}

	for deviceID, cids := range cidMap {
		if deviceID == "" {
			continue
		}
		c.removeBlocks(deviceID, cids)
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
	err = persistent.GetDB().RemoveCacheInfo(c.cacheID, c.carfileHash, isDelete, reliability)
	if err == nil {
		c.data.reliability = reliability
	}

	return err
}

// Notify nodes to delete blocks
func (c *Cache) removeBlocks(deviceID string, cids []string) {
	ctx := context.Background()

	edge := c.nodeManager.getEdgeNode(deviceID)
	if edge != nil {
		_, err := edge.nodeAPI.DeleteBlocks(ctx, cids)
		if err != nil {
			log.Errorf("removeBlocks DeleteBlocks err:%s", err.Error())
		}

		return
	}

	candidate := c.nodeManager.getCandidateNode(deviceID)
	if candidate != nil {
		_, err := candidate.nodeAPI.DeleteBlocks(ctx, cids)
		if err != nil {
			log.Errorf("removeBlocks DeleteBlocks err:%s", err.Error())
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

func (c *Cache) setCacheMessageInfo() {
	blocks, err := persistent.GetDB().GetAllBlocks(c.cacheID)
	if err != nil {
		log.Errorf("cache:%s setCacheMessage GetAllBlocks err:%s", c.cacheID, err.Error())
		return
	}

	messages := make([]*persistent.MessageInfo, 0)

	for _, block := range blocks {
		info := &persistent.MessageInfo{
			CID:        block.CID,
			Target:     block.DeviceID,
			CacheID:    block.CacheID,
			CarfileCid: c.data.carfileCid,
			Size:       block.Size,
			Type:       persistent.MsgTypeCache,
			Source:     block.Source,
			EndTime:    block.EndTime,
			CreateTime: block.CreateTime,
		}

		if block.Status == int(persistent.CacheStatusSuccess) {
			info.Status = persistent.MsgStatusSuccess
		} else {
			info.Status = persistent.MsgStatustusFail
		}

		messages = append(messages, info)
	}

	err = persistent.GetDB().SetMessageInfo(messages)
	if err != nil {
		log.Errorf("cache:%s setCacheMessage SetMessageInfo err:%s", c.cacheID, err.Error())
	}
}
