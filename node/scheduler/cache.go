package scheduler

import (
	"context"
	"fmt"

	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/node/scheduler/db/cache"
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
	id          string
	blockMap    map[string]*BlockInfo
	status      cacheStatus

	reliability int
}

// BlockInfo BlockInfo
type BlockInfo struct {
	cid        string
	deviceID   string
	deviceArea string
	deviceIP   string
	isSuccess  bool

	reliability int
}

func newCacheID(cid string) (string, error) {
	fid, err := cache.GetDB().IncrCacheID(cid)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("%s_%v", cid, fid), nil
}

func newCache(nodeManager *NodeManager, cid string) (*Cache, error) {
	id, err := newCacheID(cid)
	if err != nil {
		return nil, err
	}

	return &Cache{
		nodeManager: nodeManager,
		reliability: 0,
		status:      cacheStatusCreate,
		blockMap:    make(map[string]*BlockInfo),
		id:          id,
	}, nil
}

func (c *Cache) doCache(cid string) error {
	// deviceIDs, err := cache.GetDB().GetNodesWithCacheList(cid)
	// if err != nil {
	// 	deviceIDs = nil
	// }

	cs, _ := c.nodeManager.findCandidateNodeWithGeo(nil, nil)
	// rand node
	node := cs[randomNum(0, len(cs))]

	c.blockMap[cid] = &BlockInfo{cid: cid, deviceID: node.deviceInfo.DeviceId, deviceIP: node.addr}

	return node.nodeAPI.CacheBlocks(context.Background(), api.ReqCacheData{Cids: []string{cid}})
}
