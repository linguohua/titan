package data

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/node/helper"
	"github.com/linguohua/titan/node/scheduler/db/cache"
	"github.com/linguohua/titan/node/scheduler/db/persistent"
	"github.com/linguohua/titan/node/scheduler/node"
	"golang.org/x/xerrors"
)

// If the node disk size is greater than this value, caching will not continue
const diskUsageMax = 90

// Cache Cache
type Cache struct {
	data *Data

	cacheID     string
	carfileHash string
	status      api.CacheStatus
	reliability int
	doneSize    int
	doneBlocks  int
	totalSize   int
	totalBlocks int
	nodes       int
	isRootCache bool
	expiredTime time.Time
	lock        *sync.Mutex

	alreadyCacheBlockMap sync.Map
}

func newUUID() string {
	u2 := uuid.New()

	return strings.Replace(u2.String(), "-", "", -1)
}

func newCache(data *Data, isRootCache bool) (*Cache, string, error) {
	cacheID := newUUID()

	cache := &Cache{
		data:        data,
		reliability: 0,
		status:      api.CacheStatusCreate,
		cacheID:     cacheID,
		carfileHash: data.carfileHash,
		isRootCache: isRootCache,
		expiredTime: data.expiredTime,
		lock:        &sync.Mutex{},
		// alreadyCacheBlockMap: map[string]string{},
	}

	blockID := newUUID()
	block := &api.BlockInfo{
		CacheID:     cacheID,
		CarfileHash: data.carfileHash,
		CID:         data.carfileCid,
		CIDHash:     data.carfileHash,
		ID:          blockID,
	}

	err := persistent.GetDB().CreateCache(
		&api.CacheInfo{
			CarfileHash: cache.carfileHash,
			CacheID:     cache.cacheID,
			Status:      cache.status,
			ExpiredTime: cache.expiredTime,
			RootCache:   cache.isRootCache,
		}, block)
	if err != nil {
		return nil, "", err
	}

	return cache, blockID, err
}

func loadCache(cacheID string, data *Data) *Cache {
	if cacheID == "" {
		return nil
	}
	c := &Cache{
		cacheID: cacheID,
		data:    data,
		lock:    &sync.Mutex{},
		// alreadyCacheBlockMap: map[string]string{},
	}

	info, err := persistent.GetDB().GetCacheInfo(cacheID)
	if err != nil || info == nil {
		log.Errorf("loadCache %s, GetCacheInfo err:%v", cacheID, err)
		return nil
	}

	c.doneSize = info.DoneSize
	c.doneBlocks = info.DoneBlocks
	c.status = api.CacheStatus(info.Status)
	c.reliability = info.Reliability
	c.nodes = info.Nodes
	c.totalBlocks = info.TotalBlocks
	c.totalSize = info.TotalSize
	c.isRootCache = info.RootCache
	c.expiredTime = info.ExpiredTime
	c.carfileHash = info.CarfileHash

	// c.updateAlreadyMap()

	return c
}

func (c *Cache) updateAlreadyMap() {
	c.alreadyCacheBlockMap.Range(func(key interface{}, value interface{}) bool {
		c.alreadyCacheBlockMap.Delete(key)
		return true
	})

	// c.alreadyCacheBlockMap = map[string]string{}

	blocks, err := persistent.GetDB().GetBlocksWithStatus(c.cacheID, api.CacheStatusSuccess)
	if err != nil {
		log.Errorf("loadCache %s GetBlocksWithStatus err:%v", c.cacheID, err)
		return
	}

	for _, block := range blocks {
		// c.alreadyCacheBlockMap[block.CIDHash] = block.DeviceID
		c.alreadyCacheBlockMap.Store(block.CIDHash, block.DeviceID)
	}
}

// Notify node to cache blocks
func (c *Cache) sendBlocksToNode(deviceID string, reqDataMap map[string]*api.ReqCacheData) (int64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cNode := c.data.nodeManager.GetCandidateNode(deviceID)
	if cNode != nil {
		reqDatas := make([]api.ReqCacheData, 0)
		for _, reqData := range reqDataMap {
			reqDatas = append(reqDatas, *reqData)
		}

		// reqDatas := c.Manager.FindDownloadinfoForBlocks(blocks, c.carfileHash, c.cacheID)
		nodeCacheStat, err := cNode.GetAPI().CacheBlocks(ctx, reqDatas)
		if err != nil {
			log.Errorf("sendBlocksToNode %s, CacheBlocks err:%s", deviceID, err.Error())
		} else {
			cNode.UpdateCacheStat(&nodeCacheStat)
		}
		return cNode.GetCacheNextTimeoutTimeStamp(), err
	}

	eNode := c.data.nodeManager.GetEdgeNode(deviceID)
	if eNode != nil {
		reqDatas := make([]api.ReqCacheData, 0)
		for _, reqData := range reqDataMap {
			reqDatas = append(reqDatas, *reqData)
		}
		// reqDatas := c.Manager.FindDownloadinfoForBlocks(blocks, c.carfileHash, c.cacheID)
		nodeCacheStat, err := eNode.GetAPI().CacheBlocks(ctx, reqDatas)
		if err != nil {
			log.Errorf("sendBlocksToNode %s, CacheBlocks err:%s", deviceID, err.Error())
		} else {
			eNode.UpdateCacheStat(&nodeCacheStat)
		}

		return eNode.GetCacheNextTimeoutTimeStamp(), err
	}

	return 0, xerrors.Errorf("not found node:%s", deviceID)
}

func (c *Cache) searchAppropriateNode(skips map[string]string, index int, info *api.CacheError) (deviceID string) {
	// TODO Search strategy to be optimized
	deviceID = ""

	if c.isRootCache {
		newList := make([]*node.CandidateNode, 0)

		// candidates := c.manager.GetAllCandidate()
		c.data.nodeManager.CandidateNodeMap.Range(func(key, value interface{}) bool {
			node := value.(*node.CandidateNode)
			deviceID := node.DeviceId

			info.Nodes++

			if _, ok := skips[deviceID]; ok {
				info.SkipCount++
				return true
			}

			if node.DiskUsage >= diskUsageMax {
				info.DiskCount++
				return true
			}

			newList = append(newList, node)

			return true
		})

		if len(newList) <= 0 {
			return
		}

		sort.Slice(newList, func(i, j int) bool {
			// return newList[i].GetCacheTimeoutTimeStamp() < newList[j].GetCacheTimeoutTimeStamp()
			return newList[i].DeviceId < newList[j].DeviceId
		})

		node := newList[index%len(newList)]
		// node := newList[0]

		deviceID = node.DeviceId
		return
	}

	newList := make([]*node.EdgeNode, 0)

	// edges := c.manager.GetAllEdge()
	c.data.nodeManager.EdgeNodeMap.Range(func(key, value interface{}) bool {
		node := value.(*node.EdgeNode)
		deviceID := node.DeviceId

		info.Nodes++

		if _, ok := skips[deviceID]; ok {
			info.SkipCount++
			return true
		}

		if node.DiskUsage >= diskUsageMax {
			info.DiskCount++
			return true
		}

		newList = append(newList, node)

		return true
	})

	if len(newList) <= 0 {
		return
	}

	sort.Slice(newList, func(i, j int) bool {
		// return newList[i].GetCacheTimeoutTimeStamp() < newList[j].GetCacheTimeoutTimeStamp()
		return newList[i].DeviceId < newList[j].DeviceId
	})

	node := newList[index%len(newList)]
	// node := newList[0]

	deviceID = node.DeviceId
	return
}

func (c *Cache) findNodeAndBlockMapWithHash(hash string) (map[string]string, *node.CandidateNode, error) {
	var fromNode *node.CandidateNode

	froms, err := persistent.GetDB().GetNodesWithBlock(hash, true)
	if err == nil {
		skips := make(map[string]string)
		if froms != nil {
			for _, dID := range froms {
				skips[dID] = hash

				// find from
				if fromNode == nil {
					node := c.data.nodeManager.GetCandidateNode(dID)
					if node != nil {
						fromNode = node
					}
				}
			}
		}

		return skips, fromNode, nil
	}

	return nil, nil, err
}

// Allocate blocks to nodes
// TODO Need to refactor the function
func (c *Cache) allocateBlocksToNodes(cidMap map[string]string, isStarted bool, cacheError *api.CacheError) ([]*api.BlockInfo, map[string]map[string]*api.ReqCacheData) {
	nodeReqCacheDataMap := make(map[string]map[string]*api.ReqCacheData)
	saveDBblockList := make([]*api.BlockInfo, 0)
	cacheErrorList := make([]*api.CacheError, 0)

	if cacheError != nil {
		cacheErrorList = append(cacheErrorList, cacheError)
	}

	index := 0
	for cid, dbID := range cidMap {
		cError := &api.CacheError{CID: cid, Time: time.Now()}

		hash, err := helper.CIDString2HashString(cid)
		if err != nil {
			cError.Msg = fmt.Sprintf("cid to hash err:%s", err.Error())
			cacheErrorList = append(cacheErrorList, cError)
			continue
		}

		// if _, ok := c.alreadyCacheBlockMap[hash]; ok {
		if _, ok := c.alreadyCacheBlockMap.Load(hash); ok {
			cError.Msg = "cid already cache"
			cacheErrorList = append(cacheErrorList, cError)
			continue
		}

		status := api.CacheStatusFail
		deviceID := ""
		fromNodeID := "IPFS"
		fid := 0

		skips, fromNode, err := c.findNodeAndBlockMapWithHash(hash)
		if err != nil {
			cError.Msg = fmt.Sprintf("find hash err:%s", err.Error())
			cacheErrorList = append(cacheErrorList, cError)
		} else {
			if fromNode != nil {
				fromNodeID = fromNode.DeviceId
			}

			deviceID = c.searchAppropriateNode(skips, index, cError)
			if deviceID != "" {
				fid, err = cache.GetDB().IncrNodeCacheFid(deviceID, 1)
				if err != nil {
					cError.Msg = fmt.Sprintf("deviceID:%s,IncrNodeCacheFid err:%s", deviceID, err.Error())
					cacheErrorList = append(cacheErrorList, cError)
					continue
				}
				status = api.CacheStatusCreate

				reqDataMap, ok := nodeReqCacheDataMap[deviceID]
				if !ok {
					reqDataMap = map[string]*api.ReqCacheData{}
				}

				reqData, ok := reqDataMap[fromNodeID]
				if !ok {
					reqData = &api.ReqCacheData{}
					reqData.BlockInfos = make([]api.BlockCacheInfo, 0)
					reqData.CardFileHash = c.data.carfileHash
					reqData.CacheID = c.cacheID
					if fromNode != nil {
						reqData.DownloadURL = fromNode.GetAddress()
						reqData.DownloadToken = string(c.data.nodeManager.GetAuthToken())
					}
				}

				reqData.BlockInfos = append(reqData.BlockInfos, api.BlockCacheInfo{Cid: cid, Fid: fid})
				reqDataMap[fromNodeID] = reqData
				// cList = append(cList, &api.BlockCacheInfo{Cid: cid, Fid: fid, From: from})
				nodeReqCacheDataMap[deviceID] = reqDataMap

				// c.alreadyCacheBlockMap[hash] = deviceID
				c.alreadyCacheBlockMap.Store(hash, deviceID)

				index++
			} else {
				cacheErrorList = append(cacheErrorList, cError)
			}
		}

		b := &api.BlockInfo{
			CacheID:     c.cacheID,
			CarfileHash: c.carfileHash,
			CID:         cid,
			DeviceID:    deviceID,
			Status:      status,
			ID:          dbID,
			FID:         fid,
			Source:      fromNodeID,
			CIDHash:     hash,
			// Size:        0,
		}

		saveDBblockList = append(saveDBblockList, b)
	}

	if len(cacheErrorList) > 0 {
		err := cache.GetDB().SaveCacheErrors(c.cacheID, cacheErrorList, !isStarted)
		if err != nil {
			log.Errorf("startCache %s, SaveCacheErrors err:%s", c.cacheID, err.Error())
		}
	}

	return saveDBblockList, nodeReqCacheDataMap
}

// Notify nodes to cache blocks and setting timeout
func (c *Cache) sendBlocksToNodes(nodeCacheMap map[string]map[string]*api.ReqCacheData) {
	if nodeCacheMap == nil || len(nodeCacheMap) <= 0 {
		return
	}

	// timeStamp := time.Now().Unix()
	needTimeMax := int64(0)

	for deviceID, reqDataMap := range nodeCacheMap {
		needTime, err := c.sendBlocksToNode(deviceID, reqDataMap)
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
	if timeout <= 0 {
		timeout = 15
	}
	// update data task timeout
	c.data.dataManager.updateDataTimeout(c.carfileHash, c.cacheID, timeout, 0)

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
	var cacheError *api.CacheError

	if info.IsOK {
		c.lock.Lock()
		if api.CacheStatus(c.status) != api.CacheStatusRestore {
			if hash == c.carfileHash {
				c.totalSize = int(info.LinksSize) + info.BlockSize
				c.totalBlocks = 1
			}
			c.totalBlocks += len(info.Links)
		}
		c.doneBlocks++
		c.doneSize += info.BlockSize
		c.lock.Unlock()
		// }
		status = api.CacheStatusSuccess
		reliability = c.calculateReliability(info.DeviceID)

		//redis
		c.updateNodeBlockInfo(info.DeviceID, blockInfo.Source, info.BlockSize)

		// c.alreadyCacheBlockMap[hash] = info.DeviceID
		c.alreadyCacheBlockMap.Store(hash, info.DeviceID)
	} else {
		cacheError = &api.CacheError{
			CID:      info.Cid,
			Msg:      info.Msg,
			Time:     time.Now(),
			DeviceID: info.DeviceID,
		}
	}

	bInfo := &api.BlockInfo{
		ID:          blockInfo.ID,
		CacheID:     c.cacheID,
		CID:         blockInfo.CID,
		DeviceID:    info.DeviceID,
		Size:        info.BlockSize,
		Status:      status,
		Reliability: reliability,
		CarfileHash: c.carfileHash,
	}

	// log.Warnf("block:%s,Status:%v, link len:%d ", hash, blockInfo.Status, len(info.Links))
	linkMap := make(map[string]string)
	if len(info.Links) > 0 {
		for _, link := range info.Links {
			linkMap[link] = ""
		}
	}

	saveDbBlocks, nodeCacheMap := c.allocateBlocksToNodes(linkMap, true, cacheError)
	// save info to mysql
	err = c.data.updateAndSaveCacheingInfo(bInfo, c, saveDbBlocks)
	if err != nil {
		return xerrors.Errorf("blockCacheResult cacheID:%s,%s updateAndSaveCacheingInfo err:%s ", info.CacheID, info.Cid, err.Error())
	}

	if len(saveDbBlocks) == 0 {
		unDoneBlocks, err := persistent.GetDB().GetBlockCountWithStatus(c.cacheID, api.CacheStatusCreate)
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

	c.sendBlocksToNodes(nodeCacheMap)

	return nil
}

// update node block info in redis
func (c *Cache) updateNodeBlockInfo(deviceID, fromDeviceID string, blockSize int) {
	fromID := ""

	node := c.data.nodeManager.GetCandidateNode(fromDeviceID)
	if node != nil {
		fromID = fromDeviceID
	}

	err := cache.GetDB().UpdateNodeCacheBlockInfo(deviceID, fromID, blockSize)
	if err != nil {
		log.Errorf("UpdateNodeCacheBlockInfo err:%s", err.Error())
	}
}

func (c *Cache) startCache(cids map[string]string) error {
	saveDbBlocks, nodeCacheMap := c.allocateBlocksToNodes(cids, false, nil)

	err := persistent.GetDB().SaveCacheingResults(nil, nil, nil, saveDbBlocks)
	if err != nil {
		return xerrors.Errorf("startCache %s, SetBlockInfos err:%s", c.cacheID, err.Error())
	}

	if len(nodeCacheMap) <= 0 {
		return xerrors.Errorf("startCache %s fail not find node", c.cacheID)
	}

	err = cache.GetDB().SetDataTaskToRunningList(c.carfileHash, c.cacheID)
	if err != nil {
		return xerrors.Errorf("startCache %s , SetDataTaskToRunningList err:%s", c.carfileHash, err.Error())
	}

	err = saveEvent(c.data.carfileCid, c.cacheID, "", "", eventTypeDoCacheTaskStart)
	if err != nil {
		return xerrors.Errorf("startCache %s , saveEvent err:%s", c.carfileHash, err.Error())
	}
	// log.Infof("start cache %s,%s ---------- ", c.CarfileHash, c.CacheID)
	c.sendBlocksToNodes(nodeCacheMap)

	return nil
}

func (c *Cache) endCache(unDoneBlocks int, status api.CacheStatus) (err error) {
	// log.Infof("end cache %s,%s ----------", c.data.carfileCid, c.cacheID)
	saveEvent(c.data.carfileCid, c.cacheID, "", "", eventTypeDoCacheTaskEnd)

	err = cache.GetDB().RemoveRunningDataTask(c.carfileHash, c.cacheID)
	if err != nil {
		err = xerrors.Errorf("endCache RemoveRunningDataTask err: %s", err.Error())
		return
	}

	isContinue := true
	defer func() {
		c.data.cacheEnd(c, isContinue)
	}()

	if status == api.CacheStatusTimeout || status == api.CacheStatusFail {
		c.status = status
		isContinue = false
		return
	}

	failedBlocks, err := persistent.GetDB().GetBlockCountWithStatus(c.cacheID, api.CacheStatusFail)
	if err != nil {
		err = xerrors.Errorf("endCache %s,%s GetBlockCountWithStatus err:%v", c.data.carfileCid, c.cacheID, err.Error())
		return
	}

	if failedBlocks > 0 || unDoneBlocks > 0 {
		c.status = api.CacheStatusFail
	} else {
		c.status = api.CacheStatusSuccess
	}
	c.reliability = c.calculateReliability("")

	return
}

func (c *Cache) removeCache() error {
	err := cache.GetDB().RemoveRunningDataTask(c.carfileHash, c.cacheID)
	if err != nil {
		return xerrors.Errorf("removeCache RemoveRunningDataTask err: %s", err.Error())
	}

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

	values := make(map[string]int64, 0)
	for deviceID, cids := range cidMap {
		if deviceID == "" {
			continue
		}

		values[deviceID] = int64(-len(cids))

		go c.notifyNodeRemoveBlocks(deviceID, cids)
	}

	// update node block count
	err = cache.GetDB().IncrByDevicesInfo("BlockCount", values)
	if err != nil {
		log.Errorf("IncrByDevicesInfo err:%s ", err.Error())
	}

	if c.status == api.CacheStatusSuccess {
		c.data.reliability -= c.reliability
		err = cache.GetDB().IncrByBaseInfo("CarfileCount", -1)
		if err != nil {
			log.Errorf("removeCache IncrByBaseInfo err:%s", err.Error())
		}
	}

	isDelete := true
	c.data.CacheMap.Range(func(key, value interface{}) bool {
		if value != nil {
			ca := value.(*Cache)
			if ca != nil && c.cacheID != ca.cacheID {
				isDelete = false
			}
		}

		return true
	})

	// delete cache and update data info
	err = persistent.GetDB().RemoveCacheAndUpdateData(c.cacheID, c.carfileHash, isDelete, c.data.reliability)

	c.data.CacheMap.Delete(c.cacheID)
	c = nil

	return err
}

// Notify nodes to delete blocks
func (c *Cache) notifyNodeRemoveBlocks(deviceID string, cids []string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	edge := c.data.nodeManager.GetEdgeNode(deviceID)
	if edge != nil {
		_, err := edge.GetAPI().DeleteBlocks(ctx, cids)
		if err != nil {
			log.Errorf("notifyNodeRemoveBlocks DeleteBlocks err:%s", err.Error())
		}

		return
	}

	candidate := c.data.nodeManager.GetCandidateNode(deviceID)
	if candidate != nil {
		_, err := candidate.GetAPI().DeleteBlocks(ctx, cids)
		if err != nil {
			log.Errorf("notifyNodeRemoveBlocks DeleteBlocks err:%s", err.Error())
		}

		return
	}
}

func (c *Cache) calculateReliability(deviceID string) int {
	// TODO To be perfected
	if deviceID != "" {
		return 1
	}

	if c.status == api.CacheStatusSuccess {
		return 1
	}

	return 0
}

// GetCacheID get cache id
func (c *Cache) GetCacheID() string {
	return c.cacheID
}

// GetStatus get status
func (c *Cache) GetStatus() api.CacheStatus {
	return c.status
}

// GetDoneSize get done size
func (c *Cache) GetDoneSize() int {
	return c.doneSize
}

// GetDoneBlocks get done blocks
func (c *Cache) GetDoneBlocks() int {
	return c.doneBlocks
}

// GetNodes get nodes
func (c *Cache) GetNodes() int {
	return c.nodes
}

// IsRootCache get is root cache
func (c *Cache) IsRootCache() bool {
	return c.isRootCache
}
