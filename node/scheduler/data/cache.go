package data

import (
	"context"
	"time"

	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/node/scheduler/db/cache"
	"github.com/linguohua/titan/node/scheduler/db/persistent"
	"golang.org/x/xerrors"
)

// CacheTask CacheTask
type CacheTask struct {
	carfileRecord *CarfileRecord

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

func newCache(data *CarfileRecord, deviceID string, isRootCache bool) (*CacheTask, error) {
	cache := &CacheTask{
		carfileRecord: data,
		reliability:   0,
		status:        api.CacheStatusCreate,
		carfileHash:   data.carfileHash,
		isRootCache:   isRootCache,
		expiredTime:   data.expiredTime,
		deviceID:      deviceID,
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
func (c *CacheTask) cacheCarfile2Node() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cNode := c.carfileRecord.nodeManager.GetCandidateNode(c.deviceID)
	if cNode != nil {
		result, err := cNode.GetAPI().CacheCarfile(ctx, c.carfileRecord.carfileCid, c.carfileRecord.rootCacheDowloadInfos)
		if err != nil {
			log.Errorf("sendBlocksToNode %s, CacheCarfile err:%s", c.deviceID, err.Error())
		} else {
			c.carfileRecord.nodeManager.UpdateNodeDiskUsage(c.deviceID, result.DiskUsage)
		}
		return err
	}

	eNode := c.carfileRecord.nodeManager.GetEdgeNode(c.deviceID)
	if eNode != nil {
		result, err := eNode.GetAPI().CacheCarfile(ctx, c.carfileRecord.carfileCid, c.carfileRecord.rootCacheDowloadInfos)
		if err != nil {
			log.Errorf("sendBlocksToNode %s, CacheCarfile err:%s", c.deviceID, err.Error())
		} else {
			c.carfileRecord.nodeManager.UpdateNodeDiskUsage(c.deviceID, result.DiskUsage)
		}

		return err
	}

	return xerrors.Errorf("not found node:%s", c.deviceID)
}

func (c *CacheTask) blockCacheResult(info *api.CacheResultInfo) error {
	c.doneBlocks = info.DoneBlocks
	c.doneSize = info.DoneSize
	if info.Status == api.CacheStatusSuccess || info.Status == api.CacheStatusFail {
		//update node dick
		c.carfileRecord.nodeManager.UpdateNodeDiskUsage(c.deviceID, info.DiskUsage)

		return c.endCache(info.Status)
	}
	// update data task timeout
	err := cache.GetDB().UpdateNodeCacheingExpireTime(c.carfileHash, c.deviceID, nodeCacheResultInterval)
	if err != nil {
		log.Errorf("blockCacheResult %s , SetRunningDataTask err:%s", c.deviceID, err.Error())
	}

	return err
}

func (c *CacheTask) startCache() error {
	c.cacheCount++
	// send to node
	err := c.cacheCarfile2Node()
	if err != nil {
		return xerrors.Errorf("startCache deviceID:%s, err:%s", c.deviceID, err.Error())
	}

	err = cache.GetDB().SetCacheStart(c.carfileHash, c.deviceID, nodeCacheResultInterval)
	if err != nil {
		return xerrors.Errorf("startCache %s , SetCacheStart err:%s", c.carfileHash, err.Error())
	}

	err = saveEvent(c.carfileRecord.carfileCid, c.deviceID, "", "", eventTypeDoCacheTaskStart)
	if err != nil {
		return xerrors.Errorf("startCache %s , saveEvent err:%s", c.carfileHash, err.Error())
	}

	return nil
}

func (c *CacheTask) endCache(status api.CacheStatus) (err error) {
	saveEvent(c.carfileRecord.carfileCid, c.deviceID, "", "", eventTypeDoCacheTaskEnd)

	c.status = status
	c.reliability = c.calculateReliability("")

	err = cache.GetDB().SetCacheEnd(c.carfileHash, c.deviceID)
	if err != nil {
		return xerrors.Errorf("endCache %s , SetCacheEnd err:%s", c.carfileHash, err.Error())
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
