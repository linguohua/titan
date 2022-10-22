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

	cacheID     string
	cardFileCid string
	blockMap    map[string]*BlockInfo
	status      cacheStatus
	reliability int

	doneSize int
}

// BlockInfo BlockInfo
type BlockInfo struct {
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

	return fmt.Sprintf("cache_info_%v", fid), nil
}

func newCache(nodeManager *NodeManager, dataManager *DataManager, cid string) (*Cache, error) {
	id, err := newCacheID(cid)
	if err != nil {
		return nil, err
	}

	err = persistent.GetDB().CreateCacheInfo(id)
	if err != nil {
		return nil, err
	}

	return &Cache{
		nodeManager: nodeManager,
		dataManager: dataManager,
		reliability: 0,
		status:      cacheStatusCreate,
		blockMap:    make(map[string]*BlockInfo),
		cacheID:     id,
		cardFileCid: cid,
	}, nil
}

func loadCache(cacheID, carfileCid string, nodeManager *NodeManager, totalSize int) *Cache {
	if cacheID == "" {
		return nil
	}
	c := &Cache{
		cacheID:     cacheID,
		cardFileCid: carfileCid,
		nodeManager: nodeManager,
		blockMap:    make(map[string]*BlockInfo),
	}

	list, err := persistent.GetDB().GetCacheInfos(cacheID)
	if err == nil && list != nil {
		for _, cInfo := range list {
			c.blockMap[cInfo.CID] = &BlockInfo{
				cid:      cInfo.CID,
				deviceID: cInfo.DeviceID,
				status:   cacheStatus(cInfo.Status),
				size:     cInfo.TotalSize,
			}

			c.doneSize += cInfo.TotalSize
		}
	}

	if totalSize > 0 && c.doneSize >= totalSize {
		c.status = cacheStatusSuccess
		c.reliability = 1 // TODO
	}

	return c
}

func (c *Cache) cacheBlocks(deviceID string, cids []string) error {
	// log.Warnf("deviceID:%v,cids:%v", deviceID, cids)
	cNode := c.nodeManager.getCandidateNode(deviceID)
	if cNode != nil {
		reqDatas, _ := cNode.getReqCacheDatas(c.nodeManager, cids)
		// log.Warnf("reqDatas:%v", reqDatas)
		for _, reqData := range reqDatas {
			// log.Warnf("reqData:%v", reqData)
			err := cNode.nodeAPI.CacheBlocks(context.Background(), reqData)
			if err != nil {
				log.Errorf("edge CacheData err:%v,url:%v,cids:%v", err.Error(), reqData.CandidateURL, reqData.Cids)
			}
		}
		return nil

		// return cNode.nodeAPI.CacheBlocks(context.Background(), api.ReqCacheData{Cids: cids})
	}

	eNode := c.nodeManager.getEdgeNode(deviceID)
	if eNode != nil {
		reqDatas, _ := eNode.getReqCacheDatas(c.nodeManager, cids)
		// log.Warnf("reqDatas:%v", reqDatas)
		for _, reqData := range reqDatas {
			// log.Warnf("reqData:%v", reqData)
			err := eNode.nodeAPI.CacheBlocks(context.Background(), reqData)
			if err != nil {
				log.Errorf("edge CacheData err:%v,url:%v,cids:%v", err.Error(), reqData.CandidateURL, reqData.Cids)
			}
		}
		return nil

		// return eNode.nodeAPI.CacheBlocks(context.Background(), api.ReqCacheData{Cids: cids})
	}

	return xerrors.New(ErrNodeNotFind)
}

func (c *Cache) findNode(isHaveCache bool, filterDeviceIDs map[string]string) (deviceID, deviceAddr string, err error) {
	deviceID = ""
	deviceAddr = ""
	err = nil

	if isHaveCache {
		cs, _ := c.nodeManager.findEdgeNodeWithGeo(nil, nil, filterDeviceIDs)
		if cs == nil || len(cs) <= 0 {
			err = xerrors.New(ErrNodeNotFind)
			return
		}
		// rand node
		node := cs[randomNum(0, len(cs))]

		deviceID = node.deviceInfo.DeviceId
		deviceAddr = node.addr
		return
	} else {
		cs, _ := c.nodeManager.findCandidateNodeWithGeo(nil, nil, filterDeviceIDs)
		if cs == nil || len(cs) <= 0 {
			err = xerrors.New(ErrNodeNotFind)
			return
		}
		// rand node
		node := cs[randomNum(0, len(cs))]

		deviceID = node.deviceInfo.DeviceId
		deviceAddr = node.addr
		return
	}
}

func (c *Cache) doCache(cids []string, isHaveCache bool) error {
	// log.Warnf("doCache isHaveCache:%v", isHaveCache)
	filterDeviceIDs := make(map[string]string)
	for _, cid := range cids {
		ds, _ := persistent.GetDB().GetNodesWithCacheList(cid)
		if ds != nil {
			for _, d := range ds {
				filterDeviceIDs[d] = cid
			}
		}
	}

	deviceID, deviceAddr, err := c.findNode(isHaveCache, filterDeviceIDs)
	if err != nil {
		return err
	}

	_, cacheID := c.dataManager.getCacheTask(deviceID)
	// log.Warnf("cacheID:%v,%v", cacheID, c.cacheID)
	if cacheID != "" && cacheID != c.cacheID {
		return xerrors.New(ErrNodeNotFind)
	}

	for _, cid := range cids {
		b := &BlockInfo{cid: cid, deviceID: deviceID, deviceIP: deviceAddr, status: cacheStatusCreate, size: 0}
		c.blockMap[cid] = b
		c.saveCache(b, false)
	}

	c.dataManager.addCacheTaskMap(deviceID, c.cardFileCid, c.cacheID)

	return c.cacheBlocks(deviceID, cids)
}

func (c *Cache) saveCache(block *BlockInfo, isUpdate bool) {
	persistent.GetDB().SetCacheInfo(&persistent.CacheInfo{
		CacheID:     c.cacheID,
		CID:         block.cid,
		DeviceID:    block.deviceID,
		Status:      int(block.status),
		TotalSize:   block.size,
		Reliability: block.reliability,
	}, isUpdate)
}

func (c *Cache) updateCacheInfo(info *api.CacheResultInfo, totalSize, dataReliability int) {
	// if info.Cid != c.cardFileCid {
	// if info.LinksSize <= 0 {
	c.doneSize += info.BlockSize
	// }

	// log.Warnf("--------- totalSize:%v,%v", totalSize, c.doneSize)
	if totalSize > 0 && c.doneSize >= totalSize {
		c.status = cacheStatusSuccess
		c.reliability = 1 // TODO use block reliability
	}

	haveUndone := false

	block, ok := c.blockMap[info.Cid]
	if ok {
		if info.IsOK {
			block.status = cacheStatusSuccess
			block.reliability = 1 // TODO use device reliability
		} else {
			block.status = cacheStatusFail
		}
		block.size = info.BlockSize

		c.saveCache(block, true)

		// continue cache
		if len(info.Links) > 0 {
			haveUndone = true
			c.doCache(info.Links, dataReliability > 0)
		}
	}

	if !haveUndone {
		haveUndone, _ := persistent.GetDB().HaveUndoneCaches(c.cacheID)
		if !haveUndone && c.status != cacheStatusSuccess {
			c.status = cacheStatusFail
		}
	}
}
