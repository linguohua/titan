package scheduler

import (
	"context"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/linguohua/titan/api"
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
	data         *Data
	nodeManager  *NodeManager
	cacheID      string
	carfileCid   string
	status       cacheStatus
	reliability  int
	doneSize     int
	doneBlocks   int
	totalSize    int
	totalBlocks  int
	nodes        int
	removeBlocks int
	// dbID        int

	// timewheelCache *timewheel.TimeWheel
	// lastUpdateTime time.Time
}

func newCacheID(cid string) (string, error) {
	u2, err := uuid.NewUUID()
	if err != nil {
		return "", err
	}

	s := strings.Replace(u2.String(), "-", "", -1)
	return s, nil
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

	info, err := persistent.GetDB().GetCacheInfo(cacheID)
	// list, err := persistent.GetDB().GetCacheInfos(area, cacheID)
	if err != nil || info == nil {
		log.Errorf("loadCache %s,%s GetCacheInfo err:%v", carfileCid, cacheID, err)
		return nil
	}

	c.doneSize = info.DoneSize
	c.doneBlocks = info.DoneBlocks
	c.status = cacheStatus(info.Status)
	c.reliability = info.Reliability
	c.nodes = info.Nodes
	c.totalBlocks = info.TotalBlocks
	c.removeBlocks = info.RemoveBlocks
	c.totalSize = info.TotalSize
	// info.ExpiredTime

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

func (c *Cache) matchingNodeAndBlock(cids map[string]string, isHaveCache bool) ([]*persistent.BlockInfo, map[string][]string) {
	if cids == nil {
		return nil, nil
	}
	// c.totalBlocks += len(cids)

	nodeCacheMap := make(map[string][]string)
	blockList := make([]*persistent.BlockInfo, 0)

	i := 0
	for cid, dbID := range cids {
		i++
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

			deviceID = c.findNode(isHaveCache, filterDeviceIDs, i)
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

		isUpdate := true
		if dbID == "" {
			isUpdate = false

			u2 := uuid.NewString()
			dbID = strings.Replace(u2, "-", "", -1)
		}
		b := &persistent.BlockInfo{
			CacheID:   c.cacheID,
			CID:       cid,
			DeviceID:  deviceID,
			Status:    int(status),
			Size:      0,
			ID:        dbID,
			CarfileID: c.carfileCid,
			IsUpdate:  isUpdate,
		}

		blockList = append(blockList, b)
	}

	return blockList, nodeCacheMap
}

func (c *Cache) cacheDataToNodes(nodeCacheMap map[string][]string) {
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
	blockInfo, err := persistent.GetDB().GetBlockInfo(info.CacheID, info.Cid, info.DeviceID)
	if err != nil || blockInfo == nil {
		return xerrors.Errorf("cacheID:%s,cid:%s,deviceID:%s, GetCacheInfo err:%v", info.CacheID, info.Cid, info.DeviceID, err)
	}

	if blockInfo.Status == int(cacheStatusSuccess) {
		return xerrors.Errorf("cacheID:%s,%s block saved ", info.CacheID, info.Cid)
	}

	// err = cache.GetDB().SetRunningTask(c.carfileCid, c.cacheID)
	// if err != nil {
	// 	log.Errorf("cacheID:%s,%s SetRunningTask err:%s ", info.CacheID, info.Cid, err.Error())
	// }

	if info.Cid == c.carfileCid {
		c.totalSize = int(info.LinksSize) + info.BlockSize
		c.totalBlocks = 1
	}
	c.totalBlocks += len(info.Links)

	status := cacheStatusFail
	fid := ""
	reliability := 0
	if info.IsOK {
		c.doneBlocks++
		c.doneSize += info.BlockSize
		status = cacheStatusSuccess
		fid = info.Fid
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
		IsUpdate:    true,
	}

	linkMap := make(map[string]string)
	if len(info.Links) > 0 {
		for _, link := range info.Links {
			linkMap[link] = ""
		}
	}

	createBlocks, nodeCacheMap := c.matchingNodeAndBlock(linkMap, c.data.haveRootCache())

	// save info to db
	err = c.data.updateAndSaveInfo(bInfo, fid, info, c, createBlocks)
	if err != nil {
		return xerrors.Errorf("cacheID:%s,%s UpdateCacheInfo err:%s ", info.CacheID, info.Cid, err.Error())
	}

	unDoneBlocks, err := persistent.GetDB().GetBloackCountWithStatus(c.cacheID, int(cacheStatusCreate))
	if err != nil {
		return xerrors.Errorf("cacheID:%s,%s GetBloackCountWithStatus err:%s ", info.CacheID, info.Cid, err.Error())
	}

	if unDoneBlocks <= 0 {
		return c.endCache(unDoneBlocks)
	}

	c.cacheDataToNodes(nodeCacheMap)
	return nil
}

func (c *Cache) startCache(cids map[string]string, haveRootCache bool) error {
	createBlocks, nodeCacheMap := c.matchingNodeAndBlock(cids, haveRootCache)

	err := persistent.GetDB().SetBlockInfos(createBlocks, c.carfileCid)
	if err != nil {
		return xerrors.Errorf("startCache %s, SetBlockInfos err:%s", c.cacheID, err.Error())
	}

	if len(nodeCacheMap) <= 0 {
		return xerrors.Errorf("startCache %s fail not find node", c.cacheID)
	}

	// err = cache.GetDB().SetRunningTask(c.carfileCid, c.cacheID)
	// if err != nil {
	// 	return xerrors.Errorf("startCache %s , SetRunningTask err:%s", c.cacheID, err.Error())
	// }

	// err = cache.GetDB().SetTaskToRunningList(c.carfileCid, c.cacheID)
	// if err != nil {
	// 	return xerrors.Errorf("startCache %s , SetTaskToRunningList err:%s", c.cacheID, err.Error())
	// }

	// log.Infof("start cache %s,%s ---------- ", c.carfileCid, c.cacheID)
	c.cacheDataToNodes(nodeCacheMap)

	return nil
}

func (c *Cache) endCache(unDoneBlocks int) (err error) {
	// log.Infof("end cache %s,%s ----------", c.carfileCid, c.cacheID)

	defer func() {
		err = c.data.endData(c)
		return
	}()

	// err = cache.GetDB().RemoveRunningTask(c.carfileCid, c.cacheID)
	// if err != nil {
	// 	err = xerrors.Errorf("endCache RemoveRunningTask err: %s", err.Error())
	// 	return
	// }

	failedBlocks, err := persistent.GetDB().GetBloackCountWithStatus(c.cacheID, int(cacheStatusFail))
	if err != nil {
		err = xerrors.Errorf("endCache %s,%s GetBloackCountWithStatus err:%v", c.carfileCid, c.cacheID, err.Error())
		return
	}

	if failedBlocks > 0 || unDoneBlocks > 0 {
		c.status = cacheStatusFail
	} else {
		c.status = cacheStatusSuccess
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

	haveErr := false

	for deviceID, cids := range cidMap {
		if deviceID == "" {
			continue
		}
		nodeErrMap, err := c.removeCacheBlocks(deviceID, cids)
		if err != nil {
			log.Errorf("%s, removeCacheBlocks err:%s", deviceID, err.Error())
			haveErr = true
		}
		if len(nodeErrMap) > 0 {
			log.Errorf("%s,removeCacheBlocks errorMap:%v", deviceID, nodeErrMap)
			haveErr = true
		}
	}

	if !haveErr {
		rootCacheID := c.data.rootCacheID
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

		if c.cacheID == rootCacheID {
			rootCacheID = ""
		}
		// delete cache and update data info
		err = persistent.GetDB().RemoveAndUpdateCacheInfo(c.cacheID, c.carfileCid, rootCacheID, isDelete, reliability)
		if err == nil {
			c.data.reliability = reliability
			c.data.rootCacheID = rootCacheID
		}
	}

	return err
}

func (c *Cache) updateCacheInfos() (bool, error) {
	cCount, err := persistent.GetDB().GetBloackCountWithStatus(c.cacheID, int(cacheStatusCreate))
	if err != nil {
		return false, err
	}

	fCount, err := persistent.GetDB().GetBloackCountWithStatus(c.cacheID, int(cacheStatusFail))
	if err != nil {
		return false, err
	}

	sCount, err := persistent.GetDB().GetBloackCountWithStatus(c.cacheID, int(cacheStatusSuccess))
	if err != nil {
		return false, err
	}

	totalCount := cCount + fCount + sCount

	if totalCount == 0 {
		// remove
		return true, nil
	}

	size, err := persistent.GetDB().GetCachesSize(c.cacheID, int(cacheStatusSuccess))
	if err != nil {
		return false, err
	}

	cNodes, err := persistent.GetDB().GetNodesFromCache(c.cacheID)
	if err != nil {
		return false, err
	}

	c.doneSize = size
	c.doneBlocks = sCount
	c.nodes = cNodes

	if c.removeBlocks > 0 || totalCount > sCount {
		c.status = cacheStatusFail
		c.reliability = c.calculateReliability("")

		return false, nil
	}

	c.status = cacheStatusSuccess
	c.reliability = c.calculateReliability("")

	return false, nil
}

func (c *Cache) removeCacheBlocks(deviceID string, cids []string) (map[string]string, error) {
	ctx := context.Background()
	nodeErrs := make(map[string]string)

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
				nodeErrs[data.Cid] = fmt.Sprintf("node err:%s", data.ErrMsg)
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
				nodeErrs[data.Cid] = fmt.Sprintf("node err:%s", data.ErrMsg)
			}
		}

	}

	if !nodeFinded {
		return nil, xerrors.Errorf("%s:%s", ErrNodeNotFind, deviceID)
	}

	delRecordList := make([]string, 0)
	for _, cid := range cids {
		_, ok := nodeErrs[cid]
		if ok {
			continue
		}

		delRecordList = append(delRecordList, cid)
	}

	err := persistent.GetDB().DeleteBlockInfos(c.cacheID, deviceID, delRecordList, c.removeBlocks)
	if err != nil {
		return nodeErrs, err
	}

	c.removeBlocks += len(delRecordList)

	return nodeErrs, nil
}

func (c *Cache) calculateReliability(deviceID string) int {
	if c.status == cacheStatusSuccess {
		return 1
	}

	return 0
}
