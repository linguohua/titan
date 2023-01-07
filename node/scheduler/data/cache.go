package data

import (
	"context"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/linguohua/titan/api"
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

	deviceID    string
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
	cacheCount  int
}

func newUUID() string {
	u2 := uuid.New()

	return strings.Replace(u2.String(), "-", "", -1)
}

func newCache(data *Data, isRootCache bool) (*Cache, error) {
	cache := &Cache{
		data:        data,
		reliability: 0,
		status:      api.CacheStatusCreate,
		carfileHash: data.carfileHash,
		isRootCache: isRootCache,
		expiredTime: data.expiredTime,
		// alreadyCacheBlockMap: map[string]string{},
	}

	err := persistent.GetDB().CreateCache(
		&api.CacheInfo{
			CarfileHash: cache.carfileHash,
			DeviceID:    cache.deviceID,
			Status:      cache.status,
			ExpiredTime: cache.expiredTime,
			RootCache:   cache.isRootCache,
		})
	if err != nil {
		return nil, err
	}

	return cache, err
}

func loadCache(info *api.CacheInfo, data *Data) *Cache {
	if info == nil {
		return nil
	}
	c := &Cache{
		deviceID: info.DeviceID,
		data:     data,
	}

	c.doneSize = info.DoneSize
	c.doneBlocks = info.DoneBlocks
	c.status = api.CacheStatus(info.Status)
	c.isRootCache = info.RootCache
	c.expiredTime = info.ExpiredTime
	c.carfileHash = info.CarfileHash
	c.cacheCount = info.CacheCount

	// c.updateAlreadyMap()

	return c
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

func (c *Cache) searchAppropriateNode(filterMap map[string]string, index int, info *api.CacheError) (deviceID string) {
	// TODO Search strategy to be optimized
	deviceID = ""

	if c.isRootCache {
		newList := make([]*node.CandidateNode, 0)

		// candidates := c.manager.GetAllCandidate()
		c.data.nodeManager.CandidateNodeMap.Range(func(key, value interface{}) bool {
			node := value.(*node.CandidateNode)
			deviceID := node.DeviceId

			info.Nodes++

			if _, exist := filterMap[deviceID]; exist {
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

		if _, exist := filterMap[deviceID]; exist {
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

// func (c *Cache) findNodeAndBlockMapWithHash(hash string) (map[string]string, *node.CandidateNode, error) {
// 	var fromNode *node.CandidateNode

// 	froms, err := persistent.GetDB().GetNodesWithBlock(hash, true)
// 	if err == nil {
// 		filterMap := make(map[string]string)
// 		if froms != nil {
// 			for _, dID := range froms {
// 				filterMap[dID] = hash

// 				// find from
// 				if fromNode == nil {
// 					node := c.data.nodeManager.GetCandidateNode(dID)
// 					if node != nil {
// 						fromNode = node
// 					}
// 				}
// 			}
// 		}

// 		return filterMap, fromNode, nil
// 	}

// 	return nil, nil, err
// }

// Allocate blocks to nodes
// TODO Need to refactor the function
// func (c *Cache) allocateBlocksToNodes(cidMap map[string]string, isStarted bool, cacheError *api.CacheError) ([]*api.BlockInfo, map[string]map[string]*api.ReqCacheData) {
// 	nodeReqCacheDataMap := make(map[string]map[string]*api.ReqCacheData)
// 	saveDBblockList := make([]*api.BlockInfo, 0)
// 	cacheErrorList := make([]*api.CacheError, 0)

// 	if cacheError != nil {
// 		cacheErrorList = append(cacheErrorList, cacheError)
// 	}

// 	index := 0
// 	for cid, dbID := range cidMap {
// 		cError := &api.CacheError{CID: cid, Time: time.Now()}

// 		hash, err := helper.CIDString2HashString(cid)
// 		if err != nil {
// 			cError.Msg = fmt.Sprintf("cid to hash err:%s", err.Error())
// 			cacheErrorList = append(cacheErrorList, cError)
// 			continue
// 		}

// 		if _, exist := c.alreadyCacheBlockMap.Load(hash); exist {
// 			cError.Msg = "cid already cache"
// 			cacheErrorList = append(cacheErrorList, cError)
// 			continue
// 		}

// 		status := api.CacheStatusFail
// 		deviceID := ""
// 		fromNodeID := "IPFS"
// 		fid := 0

// 		filterMap, fromNode, err := c.findNodeAndBlockMapWithHash(hash)
// 		if err != nil {
// 			cError.Msg = fmt.Sprintf("find hash err:%s", err.Error())
// 			cacheErrorList = append(cacheErrorList, cError)
// 		} else {
// 			if fromNode != nil {
// 				fromNodeID = fromNode.DeviceId
// 			}

// 			deviceID = c.searchAppropriateNode(filterMap, index, cError)
// 			if deviceID != "" {
// 				fid, err = cache.GetDB().IncrNodeCacheFid(deviceID, 1)
// 				if err != nil {
// 					cError.Msg = fmt.Sprintf("deviceID:%s,IncrNodeCacheFid err:%s", deviceID, err.Error())
// 					cacheErrorList = append(cacheErrorList, cError)
// 					continue
// 				}
// 				status = api.CacheStatusCreate

// 				reqDataMap, exist := nodeReqCacheDataMap[deviceID]
// 				if !exist {
// 					reqDataMap = map[string]*api.ReqCacheData{}
// 				}

// 				reqData, exist := reqDataMap[fromNodeID]
// 				if !exist {
// 					reqData = &api.ReqCacheData{}
// 					reqData.BlockInfos = make([]api.BlockCacheInfo, 0)
// 					reqData.CardFileHash = c.data.carfileHash
// 					reqData.CacheID = c.deviceID
// 					if fromNode != nil {
// 						reqData.DownloadURL = fromNode.GetAddress()
// 						reqData.DownloadToken = string(c.data.nodeManager.GetAuthToken())
// 					}
// 				}

// 				reqData.BlockInfos = append(reqData.BlockInfos, api.BlockCacheInfo{Cid: cid, Fid: fid})
// 				reqDataMap[fromNodeID] = reqData
// 				// cList = append(cList, &api.BlockCacheInfo{Cid: cid, Fid: fid, From: from})
// 				nodeReqCacheDataMap[deviceID] = reqDataMap

// 				// c.alreadyCacheBlockMap[hash] = deviceID
// 				c.alreadyCacheBlockMap.Store(hash, deviceID)

// 				index++
// 			} else {
// 				cacheErrorList = append(cacheErrorList, cError)
// 			}
// 		}

// 		b := &api.BlockInfo{
// 			CacheID:     c.deviceID,
// 			CarfileHash: c.carfileHash,
// 			CID:         cid,
// 			DeviceID:    deviceID,
// 			Status:      status,
// 			ID:          dbID,
// 			FID:         fid,
// 			Source:      fromNodeID,
// 			CIDHash:     hash,
// 			// Size:        0,
// 		}

// 		saveDBblockList = append(saveDBblockList, b)
// 	}

// 	if len(cacheErrorList) > 0 {
// 		err := cache.GetDB().SaveCacheErrors(c.deviceID, cacheErrorList, !isStarted)
// 		if err != nil {
// 			log.Errorf("startCache %s, SaveCacheErrors err:%s", c.deviceID, err.Error())
// 		}
// 	}

// 	return saveDBblockList, nodeReqCacheDataMap
// }

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
	c.data.dataManager.updateDataTimeout(c.carfileHash, c.deviceID, timeout, 0)

	return
}

func (c *Cache) blockCacheResult(info *api.CacheResultInfo) error {
	// hash, err := helper.CIDString2HashString(info.Cid)
	// if err != nil {
	// 	return xerrors.Errorf("blockCacheResult %s cid to hash err:%s", info.Cid, err.Error())
	// }

	// if done
	c.endCache(api.CacheStatusCreate)
	// else update time out

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

func (c *Cache) startCache() error {
	// saveDbBlocks, nodeCacheMap := c.allocateBlocksToNodes(cids, false, nil)

	// err := persistent.GetDB().SaveCacheingResults(nil, nil, nil, saveDbBlocks)
	// if err != nil {
	// 	return xerrors.Errorf("startCache %s, SetBlockInfos err:%s", c.deviceID, err.Error())
	// }

	// if len(nodeCacheMap) <= 0 {
	// 	return xerrors.Errorf("startCache %s fail not find node", c.deviceID)
	// }

	//TODO send to node

	c.cacheCount++

	err := cache.GetDB().SetDataTaskToRunningList(c.carfileHash, c.deviceID)
	if err != nil {
		return xerrors.Errorf("startCache %s , SetDataTaskToRunningList err:%s", c.carfileHash, err.Error())
	}

	err = saveEvent(c.data.carfileCid, c.deviceID, "", "", eventTypeDoCacheTaskStart)
	if err != nil {
		return xerrors.Errorf("startCache %s , saveEvent err:%s", c.carfileHash, err.Error())
	}
	// log.Infof("start cache %s,%s ---------- ", c.CarfileHash, c.CacheID)
	// c.sendBlocksToNodes(nodeCacheMap)

	return nil
}

func (c *Cache) endCache(status api.CacheStatus) (err error) {
	// log.Infof("end cache %s,%s ----------", c.data.carfileCid, c.cacheID)
	saveEvent(c.data.carfileCid, c.deviceID, "", "", eventTypeDoCacheTaskEnd)

	err = cache.GetDB().RemoveRunningDataTask(c.carfileHash, c.deviceID)
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

	c.status = status
	c.reliability = c.calculateReliability("")

	return
}

func (c *Cache) removeCache() error {
	err := cache.GetDB().RemoveRunningDataTask(c.carfileHash, c.deviceID)
	if err != nil {
		return xerrors.Errorf("removeCache RemoveRunningDataTask err: %s", err.Error())
	}

	go c.notifyNodeRemoveBlocks(c.deviceID, []string{c.carfileHash})

	values := map[string]int64{}
	values[c.deviceID] = -int64(c.doneBlocks)
	// update node block count
	err = cache.GetDB().IncrByDevicesInfo(cache.BlockCountField, values)
	if err != nil {
		log.Errorf("IncrByDevicesInfo err:%s ", err.Error())
	}

	if c.status == api.CacheStatusSuccess {
		c.data.reliability -= c.reliability
		err = cache.GetDB().IncrByBaseInfo(cache.CarFileCountField, -1)
		if err != nil {
			log.Errorf("removeCache IncrByBaseInfo err:%s", err.Error())
		}
	}

	isDelete := true
	c.data.CacheMap.Range(func(key, value interface{}) bool {
		if value != nil {
			ca := value.(*Cache)
			if ca != nil && c.deviceID != ca.deviceID {
				isDelete = false
			}
		}

		return true
	})

	// delete cache and update data info
	err = persistent.GetDB().RemoveCacheAndUpdateData(c.deviceID, c.carfileHash, isDelete, c.data.reliability)

	c.data.CacheMap.Delete(c.deviceID)
	c = nil

	return err
}

// Notify nodes to delete blocks
func (c *Cache) notifyNodeRemoveBlocks(deviceID string, cids []string) {
	// ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	// defer cancel()

	edge := c.data.nodeManager.GetEdgeNode(deviceID)
	if edge != nil {
		_, err := edge.GetAPI().DeleteBlocks(context.Background(), cids)
		if err != nil {
			log.Errorf("notifyNodeRemoveBlocks DeleteBlocks err:%s", err.Error())
		}

		return
	}

	candidate := c.data.nodeManager.GetCandidateNode(deviceID)
	if candidate != nil {
		_, err := candidate.GetAPI().DeleteBlocks(context.Background(), cids)
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
	return c.deviceID
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
