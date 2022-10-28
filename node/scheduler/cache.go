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
type Block struct {
	cid         string
	deviceID    string
	deviceArea  string
	deviceIP    string
	status      cacheStatus
	reliability int
	size        int
}

func newCacheID(cid string) (string, error) {
	fid, err := cache.GetDB().IncrCacheID(serverArea)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("%s_cache_info_%d", serverArea, fid), nil
}

func newCache(area string, nodeManager *NodeManager, data *Data, cid string) (*Cache, error) {
	id, err := newCacheID(cid)
	if err != nil {
		return nil, err
	}

	return &Cache{
		area:        area,
		nodeManager: nodeManager,
		data:        data,
		reliability: 0,
		status:      cacheStatusCreate,
		cacheID:     id,
		cardFileCid: cid,
	}, nil
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

	info, err := persistent.GetDB().GetCacheTotalInfo(area, cacheID, carfileCid)
	// list, err := persistent.GetDB().GetCacheInfos(area, cacheID)
	if err != nil {
		log.Errorf("loadCache %s,%s GetCacheInfo err:%s", carfileCid, cacheID, err.Error())
		return c
	}

	// if list != nil {
	// 	for _, cInfo := range list {
	// 		// c.blockMap.Store(cInfo.CID, &Block{
	// 		// 	cid:      cInfo.CID,
	// 		// 	deviceID: cInfo.DeviceID,
	// 		// 	status:   cacheStatus(cInfo.Status),
	// 		// 	size:     cInfo.TotalSize,
	// 		// })

	// 		c.doneSize += cInfo.Size
	// 		if cInfo.Status == int(cacheStatusSuccess) {
	// 			c.doneBlocks++
	// 		}
	// 		c.doneBlocks = cInfo.DoneBlocks
	// 	}
	// }
	c.dbID = info.ID
	c.doneSize = info.DoneSize
	c.doneBlocks = info.DoneBlocks

	if totalSize > 0 && c.doneSize >= totalSize {
		c.status = cacheStatusSuccess
		c.reliability = 1 // TODO
	}

	return c
}

func (c *Cache) cacheBlocks(deviceID string, cids []string) error {
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

func (c *Cache) doCache(cids map[string]int, isHaveCache bool) error {
	deviceCacheMap := make(map[string][]string)
	// blockList := make([]*persistent.CacheInfo, 0)

	for cid, dbID := range cids {
		// candidateID := ""
		filterDeviceIDs := make(map[string]string)
		ds, err := persistent.GetDB().GetNodesWithCacheList(c.area, cid)
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

			// cMap[cid] = ""
			cList = append(cList, cid)
			deviceCacheMap[deviceID] = cList
		}

		// b := &Block{cid: cid, deviceID: deviceID, deviceIP: deviceAddr, status: status, size: 0}
		b := &persistent.BlockInfo{
			CacheID:  c.cacheID,
			CID:      cid,
			DeviceID: deviceID,
			Status:   int(status),
			Size:     0,
		}
		if dbID == 0 {
			c.saveBlockInfo(b, false, "")
		} else {
			b.ID = dbID
			// c.blockMap.Store(cid, b)
			c.saveBlockInfo(b, true, "")
		}
	}

	// c.saveCaches(blockList, false)

	if len(deviceCacheMap) <= 0 {
		// log.Infof("%s cache fail not find node ---------- ", c.cacheID)
		return xerrors.Errorf("cache %s fail not find node", c.cacheID)
	}

	for deviceID, caches := range deviceCacheMap {
		err := c.cacheBlocks(deviceID, caches)
		if err != nil {
			log.Errorf("cacheBlocks err:%s", err.Error())
			continue
		}
	}

	return nil
}

func (c *Cache) saveCaches(caches []*persistent.BlockInfo, isUpdate bool) {
	err := persistent.GetDB().SetCacheInfos(c.area, caches, isUpdate)
	if err != nil {
		log.Errorf("cacheID:%s,SetCacheInfos err:%v", c.cacheID, err.Error())
	}
}

func (c *Cache) saveBlockInfo(block *persistent.BlockInfo, isUpdate bool, fidStr string) {
	// log.Warnf("saveCache area:%s", c.area)
	err := persistent.GetDB().SetCacheInfo(c.area, block, isUpdate)
	if err != nil {
		log.Errorf("cacheID:%s,SetCacheInfo err:%s", c.cacheID, err.Error())
	}

	if fidStr != "" {
		err = persistent.GetDB().AddBlockInfo(serverArea, block.DeviceID, block.CID, fidStr, c.cardFileCid, c.cacheID)
		if err != nil {
			log.Errorf("cacheID:%s,AddBlockInfo err:%s", c.cacheID, err.Error())
		}
	}
}

func (c *Cache) updateBlockInfo(info *api.CacheResultInfo, totalSize, dataReliability int) map[string]int {
	cacheInfo, err := persistent.GetDB().GetCacheInfo(serverArea, info.CacheID, info.Cid)
	if err != nil || cacheInfo.CID != info.Cid || cacheInfo.DeviceID != info.DeviceID {
		log.Errorf("cid:%s,deviceID:%s, updateBlockInfo GetCacheInfo err:%v", info.Cid, info.DeviceID, err)
		return nil
	}

	status := cacheStatusFail
	fid := ""
	if info.IsOK {
		c.doneBlocks++
		c.doneSize += info.BlockSize
		status = cacheStatusSuccess
		fid = info.Fid
	}

	b := &persistent.BlockInfo{
		ID:          cacheInfo.ID,
		CacheID:     c.cacheID,
		CID:         cacheInfo.CID,
		DeviceID:    cacheInfo.DeviceID,
		Size:        info.BlockSize,
		Status:      int(status),
		Reliability: 1,
	}

	c.saveBlockInfo(b, true, fid)

	if len(info.Links) > 0 {
		m := make(map[string]int)
		for _, link := range info.Links {
			m[link] = 0
		}
		return m
	}

	haveUndone, err := persistent.GetDB().HaveCaches(c.area, c.cacheID, int(cacheStatusCreate))
	if err != nil {
		log.Errorf("HaveUndoneCaches err:%s", err.Error())
		return nil
	}

	if haveUndone {
		return nil
	}

	haveFailed, err := persistent.GetDB().HaveCaches(c.area, c.cacheID, int(cacheStatusFail))
	if haveFailed {
		c.reliability = 0
		c.status = cacheStatusFail
	} else {
		c.reliability = 1
		c.status = cacheStatusSuccess
	}

	// save total cache info
	err = persistent.GetDB().UpdateCacheTotalInfo(c.area, &persistent.BlockInfo{
		ID:          c.dbID,
		DoneSize:    c.doneSize,
		Status:      int(c.status),
		DoneBlocks:  c.doneBlocks,
		Reliability: c.reliability,
	})
	if err != nil {
		log.Errorf("cacheID:%s,UpdateCacheTotalInfo err:%s", c.cacheID, err.Error())
	}

	return nil
}
