package data

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
	"github.com/linguohua/titan/node/scheduler/errmsg"
	"github.com/linguohua/titan/node/scheduler/node"
	"golang.org/x/xerrors"
)

// Cache Cache
type Cache struct {
	Data        *Data
	Manager     *node.Manager
	CacheID     string
	CarfileHash string
	Status      api.CacheStatus
	Reliability int
	DoneSize    int
	DoneBlocks  int
	TotalSize   int
	TotalBlocks int
	Nodes       int
	IsRootCache bool
	ExpiredTime time.Time

	alreadyCacheBlockMap map[string]string
}

func newCacheID() (string, error) {
	u2 := uuid.New()

	s := strings.Replace(u2.String(), "-", "", -1)
	return s, nil
}

func newCache(nodeManager *node.Manager, data *Data, hash string, isRootCache bool) (*Cache, error) {
	id, err := newCacheID()
	if err != nil {
		return nil, err
	}

	cache := &Cache{
		Manager:              nodeManager,
		Data:                 data,
		Reliability:          0,
		Status:               api.CacheStatusCreate,
		CacheID:              id,
		CarfileHash:          hash,
		IsRootCache:          isRootCache,
		ExpiredTime:          data.ExpiredTime,
		alreadyCacheBlockMap: map[string]string{},
	}

	err = persistent.GetDB().CreateCache(
		&api.CacheInfo{
			CarfileHash: cache.CarfileHash,
			CacheID:     cache.CacheID,
			Status:      cache.Status,
			ExpiredTime: cache.ExpiredTime,
			RootCache:   cache.IsRootCache,
		})
	if err != nil {
		return nil, err
	}

	return cache, err
}

func loadCache(cacheID, carfileHash string, nodeManager *node.Manager, data *Data) *Cache {
	if cacheID == "" {
		return nil
	}
	c := &Cache{
		CacheID:              cacheID,
		CarfileHash:          carfileHash,
		Manager:              nodeManager,
		Data:                 data,
		alreadyCacheBlockMap: map[string]string{},
	}

	info, err := persistent.GetDB().GetCacheInfo(cacheID)
	if err != nil || info == nil {
		log.Errorf("loadCache %s,%s GetCacheInfo err:%v", carfileHash, cacheID, err)
		return nil
	}

	c.DoneSize = info.DoneSize
	c.DoneBlocks = info.DoneBlocks
	c.Status = api.CacheStatus(info.Status)
	c.Reliability = info.Reliability
	c.Nodes = info.Nodes
	c.TotalBlocks = info.TotalBlocks
	c.TotalSize = info.TotalSize
	c.IsRootCache = info.RootCache
	c.ExpiredTime = info.ExpiredTime

	blocks, err := persistent.GetDB().GetBlocksWithStatus(c.CacheID, api.CacheStatusSuccess)
	if err != nil {
		log.Errorf("loadCache %s,%s GetBlocksWithStatus err:%v", carfileHash, cacheID, err)
		return c
	}

	for _, block := range blocks {
		c.alreadyCacheBlockMap[block.CIDHash] = block.DeviceID
	}

	return c
}

// Notify node to cache blocks
func (c *Cache) cacheBlocksToNode(deviceID string, blocks []api.BlockCacheInfo) (int64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cNode := c.Manager.GetCandidateNode(deviceID)
	if cNode != nil {
		reqDatas := c.Manager.FindDownloadinfoForBlocks(blocks, c.CarfileHash, c.CacheID)

		nodeCacheStat, err := cNode.NodeAPI.CacheBlocks(ctx, reqDatas)
		if err != nil {
			log.Errorf("CacheBlocks %s, CacheBlocks err:%s", deviceID, err.Error())
		} else {
			cNode.UpdateCacheStat(nodeCacheStat)
		}
		return cNode.CacheNextTimeoutTimeStamp, err
	}

	eNode := c.Manager.GetEdgeNode(deviceID)
	if eNode != nil {
		reqDatas := c.Manager.FindDownloadinfoForBlocks(blocks, c.CarfileHash, c.CacheID)

		nodeCacheStat, err := eNode.NodeAPI.CacheBlocks(ctx, reqDatas)
		if err != nil {
			log.Errorf("CacheBlocks %s, CacheBlocks err:%s", deviceID, err.Error())
		} else {
			eNode.UpdateCacheStat(nodeCacheStat)
		}

		return eNode.CacheNextTimeoutTimeStamp, err
	}

	return 0, xerrors.Errorf("%s:%s", errmsg.ErrNodeNotFind, deviceID)
}

func (c *Cache) findIdleNode(skips map[string]string, i int) (deviceID string) {
	deviceID = ""

	if c.IsRootCache {
		list := c.Manager.FindCandidateNodes(nil, skips)
		if list == nil || len(list) <= 0 {
			return
		}

		newList := make([]*node.CandidateNode, 0)
		for _, node := range list {
			if node.DeviceInfo.DiskUsage < 0.9 {
				newList = append(newList, node)
			}
		}

		sort.Slice(newList, func(i, j int) bool {
			return newList[i].CacheTimeoutTimeStamp < newList[j].CacheTimeoutTimeStamp
		})

		// rand node
		node := list[i%len(list)]

		deviceID = node.DeviceInfo.DeviceId
		return
	}

	list := c.Manager.FindEdgeNodes(nil, skips)
	if list == nil || len(list) <= 0 {
		return
	}

	newList := make([]*node.EdgeNode, 0)
	for _, node := range list {
		if node.DeviceInfo.DiskUsage < 0.9 {
			newList = append(newList, node)
		}
	}

	sort.Slice(newList, func(i, j int) bool {
		return newList[i].CacheTimeoutTimeStamp < newList[j].CacheTimeoutTimeStamp
	})

	// rand node
	node := list[i%len(list)]

	deviceID = node.DeviceInfo.DeviceId
	return
}

// Allocate blocks to nodes
func (c *Cache) allocateBlocksToNodes(cids map[string]string) ([]*api.BlockInfo, map[string][]api.BlockCacheInfo) {
	nodeCacheMap := make(map[string][]api.BlockCacheInfo)
	blockList := make([]*api.BlockInfo, 0)

	i := 0
	for cid, dbID := range cids {
		hash, err := helper.CIDString2HashString(cid)
		if err != nil {
			log.Errorf("allocateBlocksToNodes %s cid to hash err:%s", cid, err.Error())
			continue
		}

		if _, ok := c.alreadyCacheBlockMap[hash]; ok {
			continue
		}

		i++
		status := api.CacheStatusFail
		deviceID := ""
		from := "IPFS"
		fid := 0

		froms, err := persistent.GetDB().GetNodesWithCache(hash, true)
		if err != nil {
			log.Errorf("allocateBlocksToNodes cache:%s,hash:%s, GetNodesWithCache err:%s", c.CacheID, hash, err.Error())
		} else {
			skips := make(map[string]string)
			if froms != nil {
				for _, dID := range froms {
					skips[dID] = cid

					// fid from
					node := c.Manager.GetCandidateNode(dID)
					if node != nil {
						from = node.DeviceInfo.DeviceId
					}
				}
			}

			deviceID = c.findIdleNode(skips, i)
			if deviceID != "" {
				status = api.CacheStatusCreate

				cList, ok := nodeCacheMap[deviceID]
				if !ok {
					cList = make([]api.BlockCacheInfo, 0)
				}

				fid, err = cache.GetDB().IncrNodeCacheFid(deviceID, 1)
				if err != nil {
					log.Errorf("deviceID:%s,IncrNodeCacheFid:%s", deviceID, err.Error())
					continue
				}

				cList = append(cList, api.BlockCacheInfo{Cid: cid, Fid: fid, From: from})
				nodeCacheMap[deviceID] = cList

				c.alreadyCacheBlockMap[hash] = deviceID
			}
		}

		b := &api.BlockInfo{
			CacheID:     c.CacheID,
			CID:         cid,
			DeviceID:    deviceID,
			Status:      status,
			Size:        0,
			ID:          dbID,
			CarfileHash: c.CarfileHash,
			FID:         fid,
			Source:      from,
			CIDHash:     hash,
		}

		blockList = append(blockList, b)
	}

	return blockList, nodeCacheMap
}

// Notify nodes to cache blocks and setting timeout
func (c *Cache) cacheBlocksToNodes(nodeCacheMap map[string][]api.BlockCacheInfo) {
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
	c.Data.DataManager.updateDataTimeout(c.CarfileHash, c.CacheID, timeout)

	return
}

func (c *Cache) blockCacheResult(info *api.CacheResultInfo) error {
	hash, err := helper.CIDString2HashString(info.Cid)
	if err != nil {
		return xerrors.Errorf("blockCacheResult %s cid to hash err:%s", info.Cid, err.Error())
	}

	blockInfo, err := persistent.GetDB().GetBlockInfo(info.CacheID, hash)
	if err != nil || blockInfo == nil {
		return xerrors.Errorf("blockCacheResult cacheID:%s,hash:%s,deviceID:%s, GetBlockInfo err:%v", info.CacheID, hash, info.DeviceID, err)
	}

	if blockInfo.Status == api.CacheStatusSuccess {
		return xerrors.Errorf("blockCacheResult cacheID:%s,%s block saved ", info.CacheID, hash)
	}

	status := api.CacheStatusFail
	reliability := 0

	if info.IsOK {
		if api.CacheStatus(c.Status) == api.CacheStatusCreate || api.CacheStatus(c.Status) == api.CacheStatusFail {
			if hash == c.CarfileHash {
				c.TotalSize = int(info.LinksSize) + info.BlockSize
				c.TotalBlocks = 1
			}
			c.TotalBlocks += len(info.Links)
		}

		c.DoneBlocks++
		c.DoneSize += info.BlockSize
		// }
		status = api.CacheStatusSuccess
		reliability = c.calculateReliability(blockInfo.DeviceID)

		c.updateNodeBlockInfo(info.DeviceID, blockInfo.Source, info.BlockSize)
	}

	// if api.CacheStatus(c.Status) != api.CacheStatusRestore {
	// 	if hash == c.CarfileHash {
	// 		c.TotalSize = int(info.LinksSize) + info.BlockSize
	// 		c.TotalBlocks = 1
	// 	}
	// 	c.TotalBlocks += len(info.Links)

	// 	if info.IsOK {
	// 		c.DoneBlocks++
	// 		c.DoneSize += info.BlockSize
	// 		status = api.CacheStatusSuccess
	// 		reliability = c.calculateReliability(blockInfo.DeviceID)

	// 		c.updateNodeBlockInfo(info.DeviceID, blockInfo.Source, info.BlockSize)
	// 	}
	// } else {
	// 	// info.Links = make([]string, 0)

	// 	if info.IsOK {
	// 		status = api.CacheStatusSuccess
	// 		reliability = c.calculateReliability(blockInfo.DeviceID)

	// 		c.updateNodeBlockInfo(info.DeviceID, blockInfo.Source, info.BlockSize)
	// 	}
	// }

	bInfo := &api.BlockInfo{
		ID:          blockInfo.ID,
		CacheID:     c.CacheID,
		CID:         blockInfo.CID,
		DeviceID:    blockInfo.DeviceID,
		Size:        info.BlockSize,
		Status:      status,
		Reliability: reliability,
		CarfileHash: c.CarfileHash,
	}

	// log.Warnf("block:%s,Status:%v, link len:%d ", hash, blockInfo.Status, len(info.Links))
	linkMap := make(map[string]string)
	if len(info.Links) > 0 {
		for _, link := range info.Links {
			linkMap[link] = ""
		}
	}

	saveDbBlocks, nodeCacheMap := c.allocateBlocksToNodes(linkMap)
	// save info to db
	err = c.Data.updateAndSaveCacheingInfo(bInfo, c, saveDbBlocks)
	if err != nil {
		return xerrors.Errorf("blockCacheResult cacheID:%s,%s updateAndSaveCacheingInfo err:%s ", info.CacheID, info.Cid, err.Error())
	}

	if len(saveDbBlocks) == 0 {
		unDoneBlocks, err := persistent.GetDB().GetBlockCountWithStatus(c.CacheID, api.CacheStatusCreate)
		if err != nil {
			return xerrors.Errorf("blockCacheResult cacheID:%s,%s GetBlockCountWithStatus err:%s ", info.CacheID, info.Cid, err.Error())
		}

		// restoreBlocks, err := persistent.GetDB().GetBlockCountWithStatus(c.CacheID, api.CacheStatusRestore)
		// if err != nil {
		// 	return xerrors.Errorf("blockCacheResult cacheID:%s,%s GetBlockCountWithStatus err:%s ", info.CacheID, info.Cid, err.Error())
		// }

		// unDoneBlocks += restoreBlocks

		if unDoneBlocks <= 0 {
			return c.endCache(unDoneBlocks, api.CacheStatusCreate)
		}
	}

	c.cacheBlocksToNodes(nodeCacheMap)

	return nil
}

func (c *Cache) updateNodeBlockInfo(deviceID, fromID string, blockSize int) {
	err := cache.GetDB().UpdateDeviceInfo(deviceID, func(deviceInfo *api.DevicesInfo) {
		deviceInfo.BlockCount++
		deviceInfo.TotalDownload += float64(blockSize)
	})
	if err != nil {
		log.Warnf("UpdateDeviceInfo err:%s", err.Error())
	}

	node := c.Data.NodeManager.GetCandidateNode(fromID)
	if node == nil {
		return
	}

	err = cache.GetDB().UpdateDeviceInfo(node.DeviceInfo.DeviceId, func(deviceInfo *api.DevicesInfo) {
		deviceInfo.DownloadCount++
		deviceInfo.TotalUpload += float64(blockSize)
	})
	if err != nil {
		log.Warnf("UpdateDeviceInfo err:%s", err.Error())
	}
}

func (c *Cache) startCache(cids map[string]string) error {
	saveDbBlocks, nodeCacheMap := c.allocateBlocksToNodes(cids)

	err := persistent.GetDB().SaveCacheingResults(nil, nil, nil, saveDbBlocks)
	if err != nil {
		return xerrors.Errorf("startCache %s, SetBlockInfos err:%s", c.CacheID, err.Error())
	}

	if len(nodeCacheMap) <= 0 {
		return xerrors.Errorf("startCache %s fail not find node", c.CacheID)
	}

	// log.Infof("start cache %s,%s ---------- ", c.CarfileHash, c.CacheID)
	c.cacheBlocksToNodes(nodeCacheMap)

	c.Data.DataManager.saveEvent(c.Data.CarfileCid, c.CacheID, "", "", eventTypeDoCacheTaskStart)

	err = cache.GetDB().SetDataTaskToRunningList(c.CarfileHash, c.CacheID)
	if err != nil {
		return xerrors.Errorf("startCache %s , SetDataTaskToRunningList err:%s", c.CarfileHash, err.Error())
	}

	return nil
}

func (c *Cache) endCache(unDoneBlocks int, status api.CacheStatus) (err error) {
	// log.Infof("end cache %s,%s ----------", c.data.carfileCid, c.cacheID)
	c.Data.DataManager.saveEvent(c.Data.CarfileCid, c.CacheID, "", "", eventTypeDoCacheTaskEnd)

	err = cache.GetDB().RemoveRunningDataTask(c.CarfileHash, c.CacheID)
	if err != nil {
		err = xerrors.Errorf("endCache RemoveRunningDataTask err: %s", err.Error())
		return
	}

	defer func() {
		c.Data.cacheEnd(c)

		if c.Status == api.CacheStatusSuccess {
			err = cache.GetDB().UpdateSystemInfo(func(info *api.BaseInfo) {
				info.CarFileCount++
			})
			if err != nil {
				log.Errorf("endCache UpdateSystemInfo err: %s", err.Error())
			}
		}
	}()

	if status == api.CacheStatusTimeout || status == api.CacheStatusFail {
		c.Status = status
		return
	}

	failedBlocks, err := persistent.GetDB().GetBlockCountWithStatus(c.CacheID, api.CacheStatusFail)
	if err != nil {
		err = xerrors.Errorf("endCache %s,%s GetBlockCountWithStatus err:%v", c.Data.CarfileCid, c.CacheID, err.Error())
		return
	}

	if failedBlocks > 0 || unDoneBlocks > 0 {
		c.Status = api.CacheStatusFail
	} else {
		c.Status = api.CacheStatusSuccess
	}
	c.Reliability = c.calculateReliability("")

	return
}

func (c *Cache) removeCache() error {
	err := cache.GetDB().RemoveRunningDataTask(c.CarfileHash, c.CacheID)
	if err != nil {
		err = xerrors.Errorf("removeCache RemoveRunningDataTask err: %s", err.Error())
	}

	reliability := c.Data.Reliability

	blocks, err := persistent.GetDB().GetAllBlocks(c.CacheID)
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

		// update node block count
		err = cache.GetDB().UpdateDeviceInfo(deviceID, func(deviceInfo *api.DevicesInfo) {
			deviceInfo.BlockCount -= len(cids)
		})
		if err != nil {
			log.Errorf("UpdateDeviceInfo err:%s ", err.Error())
		}

		go c.removeBlocks(deviceID, cids)
	}

	reliability -= c.Reliability

	c.Data.CacheMap.Delete(c.CacheID)

	isDelete := true
	c.Data.CacheMap.Range(func(key, value interface{}) bool {
		if value != nil {
			c := value.(*Cache)
			if c != nil {
				isDelete = false
			}
		}

		return true
	})

	// delete cache and update data info
	err = persistent.GetDB().RemoveCacheInfo(c.CacheID, c.CarfileHash, isDelete, reliability)
	if err == nil {
		c.Data.Reliability = reliability
	}

	if c.Status == api.CacheStatusSuccess {
		err = cache.GetDB().UpdateSystemInfo(func(info *api.BaseInfo) {
			info.CarFileCount--
		})
	}

	return err
}

// Notify nodes to delete blocks
func (c *Cache) removeBlocks(deviceID string, cids []string) {
	ctx := context.Background()

	edge := c.Manager.GetEdgeNode(deviceID)
	if edge != nil {
		_, err := edge.NodeAPI.DeleteBlocks(ctx, cids)
		if err != nil {
			log.Errorf("removeBlocks DeleteBlocks err:%s", err.Error())
		}

		return
	}

	candidate := c.Manager.GetCandidateNode(deviceID)
	if candidate != nil {
		_, err := candidate.NodeAPI.DeleteBlocks(ctx, cids)
		if err != nil {
			log.Errorf("removeBlocks DeleteBlocks err:%s", err.Error())
		}

		return
	}
}

func (c *Cache) calculateReliability(deviceID string) int {
	if deviceID != "" {
		return 1
	}

	if c.Status == api.CacheStatusSuccess {
		return 1
	}

	return 0
}

// func (c *Cache) setCacheMessageInfo() {
// 	blocks, err := persistent.GetDB().GetAllBlocks(c.cacheID)
// 	if err != nil {
// 		log.Errorf("cache:%s setCacheMessage GetAllBlocks err:%s", c.cacheID, err.Error())
// 		return
// 	}

// 	messages := make([]*persistent.MessageInfo, 0)

// 	for _, block := range blocks {
// 		info := &persistent.MessageInfo{
// 			CID:        block.CID,
// 			Target:     block.DeviceID,
// 			CacheID:    block.CacheID,
// 			CarfileCid: c.data.carfileCid,
// 			Size:       block.Size,
// 			Type:       persistent.MsgTypeCache,
// 			Source:     block.Source,
// 			EndTime:    block.EndTime,
// 			CreateTime: block.CreateTime,
// 		}

// 		if block.Status == int(persistent.CacheStatusSuccess) {
// 			info.Status = persistent.MsgStatusSuccess
// 		} else {
// 			info.Status = persistent.MsgStatustusFail
// 		}

// 		messages = append(messages, info)
// 	}

// 	err = persistent.GetDB().SetMessageInfo(messages)
// 	if err != nil {
// 		log.Errorf("cache:%s setCacheMessage SetMessageInfo err:%s", c.cacheID, err.Error())
// 	}
// }

func (c *Cache) replenishExpiredTime(hour int) {
	c.ExpiredTime = c.ExpiredTime.Add((time.Duration(hour) * time.Hour))

	err := persistent.GetDB().ChangeExpiredTimeWhitCaches(c.CarfileHash, c.CacheID, c.ExpiredTime)
	if err != nil {
		log.Errorf("ChangeExpiredTimeWhitCaches err:%s", err.Error())
	}
}

func (c *Cache) resetExpiredTime(expiredTime time.Time) {
	c.ExpiredTime = expiredTime

	err := persistent.GetDB().ChangeExpiredTimeWhitCaches(c.CarfileHash, c.CacheID, c.ExpiredTime)
	if err != nil {
		log.Errorf("ChangeExpiredTimeWhitCaches err:%s", err.Error())
	}
}
