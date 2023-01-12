package carfile

import (
	"context"
	"time"

	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/node/scheduler/db/cache"
	"github.com/linguohua/titan/node/scheduler/db/persistent"
	"github.com/linguohua/titan/node/scheduler/node"
	"golang.org/x/xerrors"
)

// CacheTask CacheTask
type CacheTask struct {
	carfileRecord *CarfileRecord

	node        *node.Node
	deviceID    string
	carfileHash string
	status      api.CacheStatus
	reliability int
	doneSize    int
	doneBlocks  int
	totalSize   int
	totalBlocks int
	isRootCache bool
	expiredTime time.Time
	cacheCount  int
}

func newCache(data *CarfileRecord, node *node.Node, isRootCache bool) (*CacheTask, error) {
	cache := &CacheTask{
		carfileRecord: data,
		reliability:   0,
		status:        api.CacheStatusCreate,
		carfileHash:   data.carfileHash,
		isRootCache:   isRootCache,
		expiredTime:   data.expiredTime,
		deviceID:      node.DeviceId,
		node:          node,
	}

	err := persistent.GetDB().CreateCache(
		&api.CacheTaskInfo{
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

// Notify node to cache blocks
func (c *CacheTask) cacheCarfile2Node() (api.CacheCarfileResult, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cNode := c.carfileRecord.nodeManager.GetCandidateNode(c.deviceID)
	if cNode != nil {
		c.node = cNode.Node
		return cNode.GetAPI().CacheCarfile(ctx, c.carfileRecord.carfileCid, c.carfileRecord.rootCacheDowloadInfos)
	}

	eNode := c.carfileRecord.nodeManager.GetEdgeNode(c.deviceID)
	if eNode != nil {
		c.node = eNode.Node
		return eNode.GetAPI().CacheCarfile(ctx, c.carfileRecord.carfileCid, c.carfileRecord.rootCacheDowloadInfos)
	}

	return api.CacheCarfileResult{}, xerrors.Errorf("not found node:%s", c.deviceID)
}

func (c *CacheTask) blockCacheResult(info *api.CacheResultInfo) error {
	c.doneBlocks = info.DoneBlocks
	c.doneSize = info.DoneSize

	c.totalBlocks = info.TotalBlock
	c.totalSize = info.TotalSize

	if !c.carfileRecord.existRootCache() {
		c.carfileRecord.totalSize = c.totalSize
		c.carfileRecord.totalBlocks = c.totalBlocks
	}

	log.Warnf("blockCacheResult :%s , %d", c.deviceID, info.Status)

	if info.Status == api.CacheStatusSuccess || info.Status == api.CacheStatusFail {
		//update node dick
		// c.carfileRecord.nodeManager.UpdateNodeDiskUsage(c.deviceID, info.DiskUsage)
		c.node.DiskUsage = info.DiskUsage
		c.node.IncrCurCacheCount(-1)

		return c.endCache(info.Status)
	}

	if err := c.carfileRecord.updateAndSaveCacheInfo(c); err != nil {
		return err
	}

	// update data task timeout
	return cache.GetDB().UpdateNodeCacheingExpireTime(c.carfileHash, c.deviceID, nodeCacheResultInterval)
}

func (c *CacheTask) startCache() error {
	c.cacheCount++
	// send to node
	result, err := c.cacheCarfile2Node()
	if err != nil {
		return xerrors.Errorf("startCache deviceID:%s, err:%s", c.deviceID, err.Error())
	}

	c.node.DiskUsage = result.DiskUsage
	c.node.SetCurCacheCount(result.DoingCacheCarfileNum + result.WaitCacheCarfileNum)

	err = cache.GetDB().SetCacheTaskStart(c.carfileHash, c.deviceID, nodeCacheResultInterval)
	if err != nil {
		return xerrors.Errorf("startCache %s , SetCacheTaskStart err:%s", c.carfileHash, err.Error())
	}

	return nil
}

func (c *CacheTask) endCache(status api.CacheStatus) (err error) {
	c.status = status
	c.reliability = c.calculateReliability("")

	err = cache.GetDB().SetCacheTaskEnd(c.carfileHash, c.deviceID)
	if err != nil {
		return xerrors.Errorf("endCache %s , SetCacheTaskEnd err:%s", c.carfileHash, err.Error())
	}

	return c.carfileRecord.cacheDone(c)
}

func (c *CacheTask) calculateReliability(deviceID string) int {
	// TODO To be perfected
	if deviceID != "" {
		return 1
	}

	if !c.isRootCache && c.status == api.CacheStatusSuccess {
		return 1
	}

	return 0
}

// GetDeviceID get device id
func (c *CacheTask) GetDeviceID() string {
	return c.deviceID
}

// GetStatus get status
func (c *CacheTask) GetStatus() api.CacheStatus {
	return c.status
}

// GetDoneSize get done size
func (c *CacheTask) GetDoneSize() int {
	return c.doneSize
}

// GetDoneBlocks get done blocks
func (c *CacheTask) GetDoneBlocks() int {
	return c.doneBlocks
}

// IsRootCache get is root cache
func (c *CacheTask) IsRootCache() bool {
	return c.isRootCache
}
