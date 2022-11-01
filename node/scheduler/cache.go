package scheduler

import (
	"context"
	"fmt"

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
	area        string
	cacheID     string
	cardFileCid string
	// blockMap    sync.Map
	status      cacheStatus
	reliability int
	doneSize    int
	doneBlocks  int
	dbID        int
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

	return fmt.Sprintf("%s_cache_info_%d", aName, fid), nil
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
		cardFileCid: cid,
	}

	// // save total cache info
	// err = persistent.GetDB().SetCacheInfo(&persistent.CacheInfo{
	// 	CarfileID: cache.cardFileCid,
	// 	CacheID:   cache.cacheID,
	// 	Status:    int(cache.status),
	// })
	// if err != nil {
	// 	log.Errorf("cacheID:%s,SetCacheInfo err:%s", cache.cacheID, err.Error())
	// }

	return cache, err
}

func loadCache(area, cacheID, carfileCid string, nodeManager *NodeManager, totalSize int) *Cache {
	if cacheID == "" {
		return nil
	}
	c := &Cache{
		area:        area,
		cacheID:     cacheID,
		cardFileCid: carfileCid,
		nodeManager: nodeManager,
	}

	info, err := persistent.GetDB().GetCacheInfo(cacheID, carfileCid)
	// list, err := persistent.GetDB().GetCacheInfos(area, cacheID)
	if err != nil || info == nil {
		log.Errorf("loadCache %s,%s GetCacheInfo err:%v", carfileCid, cacheID, err)
		return c
	}

	c.dbID = info.ID
	c.doneSize = info.DoneSize
	c.doneBlocks = info.DoneBlocks
	c.status = cacheStatus(info.Status)
	c.reliability = info.Reliability // TODO

	return c
}

func (c *Cache) cacheBlocksToNode(deviceID string, cids []string) error {
	cNode := c.nodeManager.getCandidateNode(deviceID)
	if cNode != nil {
		reqDatas := cNode.getReqCacheDatas(c.nodeManager, cids, c.cardFileCid, c.cacheID)

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
		reqDatas := eNode.getReqCacheDatas(c.nodeManager, cids, c.cardFileCid, c.cacheID)

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

// func (c *Cache) saveBlockInfo(block *persistent.BlockInfo, isUpdate bool, fidStr string) {
// 	// log.Warnf("saveCache area:%s", c.area)
// 	err := persistent.GetDB().SetBlockInfo(block, c.cardFileCid, fidStr, isUpdate)
// 	if err != nil {
// 		log.Errorf("cacheID:%s,SetBlockInfo err:%s", c.cacheID, err.Error())
// 	}
// }

func (c *Cache) blockCacheResult(info *api.CacheResultInfo) (*persistent.BlockInfo, map[string]int, error) {
	// log.Warnf("updateBlockInfo device:%s,fid:%s ", info.DeviceID, info.Fid)
	cacheInfo, err := persistent.GetDB().GetBlockInfo(info.CacheID, info.Cid, info.DeviceID)
	if err != nil || cacheInfo == nil {
		return nil, nil, xerrors.Errorf("CacheID:%s,cid:%s,deviceID:%s, updateBlockInfo GetCacheInfo err:%v", info.CacheID, info.Cid, info.DeviceID, err)
	}

	if cacheInfo.Status == int(cacheStatusSuccess) {
		return nil, nil, xerrors.Errorf("%s block saved ", info.Cid)
	}

	status := cacheStatusFail
	// fid := ""
	if info.IsOK {
		c.doneBlocks++
		c.doneSize += info.BlockSize
		status = cacheStatusSuccess
		// fid = info.Fid
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

	if len(info.Links) > 0 {
		m := make(map[string]int)
		for _, link := range info.Links {
			m[link] = 0
		}
		return bInfo, m, nil
	}

	count, err := persistent.GetDB().HaveBlocks(c.cacheID, int(cacheStatusCreate))
	if err != nil {
		return bInfo, nil, xerrors.Errorf("HaveUndoneCaches err:%s", err.Error())
	}

	if count > 1 {
		return bInfo, nil, nil
	}

	return bInfo, nil, c.endCache(status)
}

func (c *Cache) startCache(haveRootCache bool) error {
	log.Infof("%s cache start ---------- ", c.cacheID)

	createBlocks, deviceCacheMap := c.getCacheInfos(map[string]int{c.cardFileCid: 0}, haveRootCache)

	c.saveBlockInfos(createBlocks)
	if len(deviceCacheMap) <= 0 {
		// log.Infof("%s cache fail not find node ---------- ", c.cacheID)
		return xerrors.Errorf("cache %s fail not find node", c.cacheID)
	}

	c.cacheToNodes(deviceCacheMap)

	return nil
}

func (c *Cache) endCache(endBlockStatus cacheStatus) error {
	log.Infof("%s cache end ---------- ", c.cacheID)

	count, err := persistent.GetDB().HaveBlocks(c.cacheID, int(cacheStatusFail))
	if err != nil {
		return err
	}

	if endBlockStatus == cacheStatusFail {
		count++
	}

	if count > 0 {
		c.reliability = 0
		c.status = cacheStatusFail
	} else {
		c.reliability = 1
		c.status = cacheStatusSuccess
	}

	return nil
}
