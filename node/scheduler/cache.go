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
	nodeManager *NodeManager
	dataManager *DataManager
	area        string
	cacheID     string
	cardFileCid string
	// blockMap    sync.Map
	status      cacheStatus
	reliability int
	doneSize    int
	doneBlocks  int
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
	fid, err := cache.GetDB().IncrCacheID()
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("cache_info_%d", fid), nil
}

func newCache(area string, nodeManager *NodeManager, dataManager *DataManager, cid string) (*Cache, error) {
	id, err := newCacheID(cid)
	if err != nil {
		return nil, err
	}

	return &Cache{
		area:        area,
		nodeManager: nodeManager,
		dataManager: dataManager,
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

	list, err := persistent.GetDB().GetCacheInfos(area, cacheID)
	if err != nil {
		log.Errorf("loadCache %s,%s GetCacheInfos err:%s", carfileCid, cacheID, err.Error())
		return c
	}

	if list != nil {
		for _, cInfo := range list {
			// c.blockMap.Store(cInfo.CID, &Block{
			// 	cid:      cInfo.CID,
			// 	deviceID: cInfo.DeviceID,
			// 	status:   cacheStatus(cInfo.Status),
			// 	size:     cInfo.TotalSize,
			// })

			c.doneSize += cInfo.TotalSize
			if cInfo.Status == int(cacheStatusSuccess) {
				c.doneBlocks++
			}
		}
	}

	if totalSize > 0 && c.doneSize >= totalSize {
		c.status = cacheStatusSuccess
		c.reliability = 1 // TODO
	}

	return c
}

func (c *Cache) cacheBlocks(deviceID string, cids []string) error {
	cNode := c.nodeManager.getCandidateNode(deviceID)
	if cNode != nil {
		reqDatas, list := cNode.getReqCacheDatas(c.nodeManager, cids, c.cardFileCid, c.cacheID)
		if len(list) > 0 {
			reqDatas = append(reqDatas, api.ReqCacheData{Cids: list, CardFileCid: c.cardFileCid, CacheID: c.cacheID})
		}

		for _, reqData := range reqDatas {
			err := cNode.nodeAPI.CacheBlocks(context.Background(), reqData)
			if err != nil {
				log.Errorf("candidate CacheData err:%s,url:%s,cids:%v", err.Error(), reqData.CandidateURL, reqData.Cids)
			}
		}
		return nil
	}

	eNode := c.nodeManager.getEdgeNode(deviceID)
	if eNode != nil {
		reqDatas, list := eNode.getReqCacheDatas(c.nodeManager, cids, c.cardFileCid, c.cacheID)
		if len(list) > 0 {
			reqDatas = append(reqDatas, api.ReqCacheData{Cids: list, CardFileCid: c.cardFileCid, CacheID: c.cacheID})
		}

		for _, reqData := range reqDatas {
			err := eNode.nodeAPI.CacheBlocks(context.Background(), reqData)
			if err != nil {
				log.Errorf("edge CacheData err:%s,url:%s,cids:%v", err.Error(), reqData.CandidateURL, reqData.Cids)
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
	node := cs[randomNum(0, len(cs))]

	deviceID = node.deviceInfo.DeviceId
	deviceAddr = node.addr
	return
}

func (c *Cache) doCache(cids []string, isHaveCache bool) {
	deviceCacheMap := make(map[string][]string)
	// blockList := make([]*persistent.CacheInfo, 0)

	for i, cid := range cids {
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

		deviceID, _ := c.findNode(isHaveCache, filterDeviceIDs, i)
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
			CacheID:   c.cacheID,
			CID:       cid,
			DeviceID:  deviceID,
			Status:    int(status),
			TotalSize: 0,
		}
		// c.blockMap.Store(cid, b)
		c.saveBlockInfo(b)

		// blockList = append(blockList, &persistent.CacheInfo{
		// 	CacheID:     c.cacheID,
		// 	CID:         b.cid,
		// 	DeviceID:    b.deviceID,
		// 	Status:      int(b.status),
		// 	TotalSize:   b.size,
		// 	Reliability: b.reliability,
		// })
	}

	// c.saveCaches(blockList, false)

	if len(deviceCacheMap) <= 0 {
		log.Infof("%s cache fail not find node ---------- ", c.cacheID)
		return
	}

	for deviceID, caches := range deviceCacheMap {
		err := c.cacheBlocks(deviceID, caches)
		if err != nil {
			log.Errorf("cacheBlocks err:%s", err.Error())
			continue
		}
	}

	return
}

func (c *Cache) saveCaches(caches []*persistent.BlockInfo, isUpdate bool) {
	err := persistent.GetDB().SetCacheInfos(c.area, caches, isUpdate)
	if err != nil {
		log.Errorf("cacheID:%s,SetCacheInfos err:%v", c.cacheID, err.Error())
	}
}

func (c *Cache) saveBlockInfo(block *persistent.BlockInfo) {
	// log.Warnf("saveCache area:%s", c.area)
	err := persistent.GetDB().SetCacheInfo(c.area, block)
	if err != nil {
		log.Errorf("cacheID:%s,SetCacheInfo err:%s", c.cacheID, err.Error())
	}
}

func (c *Cache) updateBlockInfo(info *api.CacheResultInfo, totalSize, dataReliability int) []string {
	cacheInfo, err := persistent.GetDB().GetCacheInfo2(serverArea, info.DbID)
	if err != nil || cacheInfo.ID != info.DbID || cacheInfo.CID != info.Cid || cacheInfo.DeviceID != info.DeviceID {
		log.Errorf("dbID:%d,cid:%s,deviceID:%s, updateBlockInfo getcacheinfo err:%v", info.DbID, info.Cid, info.DeviceID, err)
		return nil
	}

	status := cacheStatusFail
	if info.IsOK {
		c.doneBlocks++
		c.doneSize += info.BlockSize
		status = cacheStatusSuccess
	}

	if totalSize > 0 && c.doneSize >= totalSize {
		c.status = cacheStatusSuccess
		c.reliability = 1 // TODO use block reliability

		log.Infof("%s cache done ---------- ", c.cacheID)
	}

	b := &persistent.BlockInfo{
		ID:          cacheInfo.ID,
		CacheID:     c.cacheID,
		CID:         cacheInfo.CID,
		DeviceID:    cacheInfo.DeviceID,
		TotalSize:   info.BlockSize,
		Status:      int(status),
		Reliability: 1,
	}
	c.saveBlockInfo(b)

	if len(info.Links) > 0 {
		return info.Links
	}

	haveUndone, err := persistent.GetDB().HaveUndoneCaches(c.area, c.cacheID)
	if err != nil {
		log.Errorf("HaveUndoneCaches err:%s", err.Error())
		return nil
	}
	if !haveUndone && c.status != cacheStatusSuccess {
		c.status = cacheStatusFail

		log.Infof("%s cache fail ---------- ", c.cacheID)
	}

	return nil
}
