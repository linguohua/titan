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
	cacheStatusTimeOut
	cacheStatusSuccess
	cacheStatusFail
	cacheStatusOther
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
	isSuccess   bool
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

func (c *Cache) doCache(cids []string) error {
	// deviceIDs, err := cache.GetDB().GetNodesWithCacheList(cid)
	// if err != nil {
	// 	deviceIDs = nil
	// }

	cs, _ := c.nodeManager.findCandidateNodeWithGeo(nil, nil)
	// rand node
	node := cs[randomNum(0, len(cs))]

	task := c.dataManager.getCacheTask(node.deviceInfo.DeviceId)
	if task != nil && task.cacheID != c.cacheID {
		return xerrors.New(ErrNodeNotFind)
	}

	for _, cid := range cids {
		b := &BlockInfo{cid: cid, deviceID: node.deviceInfo.DeviceId, deviceIP: node.addr, isSuccess: false, size: 0}
		c.blockMap[cid] = b
		c.saveCache(b, false)
	}

	c.dataManager.addCacheTaskMap(node.deviceInfo.DeviceId, c.cardFileCid, c.cacheID)

	err := node.nodeAPI.CacheBlocks(context.Background(), api.ReqCacheData{Cids: cids, CardFileCid: c.cardFileCid})

	return err
}

func (c *Cache) saveCache(block *BlockInfo, isUpdate bool) {
	status := 0
	if block.isSuccess {
		status = 1
	}
	persistent.GetDB().SetCacheInfo(&persistent.CacheInfo{
		CacheID:   c.cacheID,
		CID:       block.cid,
		DeviceID:  block.deviceID,
		Status:    status,
		TotalSize: block.size,
	}, isUpdate)
}
