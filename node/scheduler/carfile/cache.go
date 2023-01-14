package carfile

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
	doneSize    int64
	doneBlocks  int
	// totalSize    int64
	// totalBlocks  int
	isRootCache  bool
	expiredTime  time.Time
	executeCount int

	timeoutTicker *time.Ticker
}

func newCache(carfileRecord *CarfileRecord, deviceID string, isRootCache bool) (*CacheTask, error) {
	cache := &CacheTask{
		carfileRecord: carfileRecord,
		reliability:   0,
		status:        api.CacheStatusCreate,
		carfileHash:   carfileRecord.carfileHash,
		isRootCache:   isRootCache,
		expiredTime:   carfileRecord.expiredTime,
		deviceID:      deviceID,
	}

	err := persistent.GetDB().CreateCacheInfo(
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

func (c *CacheTask) startTimeoutTimer() {
	c.timeoutTicker = time.NewTicker(time.Duration(checkCacheTimeoutInterval) * time.Second)
	defer c.timeoutTicker.Stop()

	for {
		select {
		case <-c.timeoutTicker.C:
			if c.status != api.CacheStatusCreate {
				return
			}
			//check redis
			exist, err := cache.GetDB().IsNodeCaching(c.deviceID)
			if err != nil {
				log.Errorf("NodeIsCaching err:%s", err.Error())
				break
			}

			log.Infof("check timeout device:%s , exist:%v", c.deviceID, exist)
			if exist {
				break
			}

			//cache is timeout
			err = c.endCache(api.CacheStatusTimeout)
			if err != nil {
				log.Errorf("endCache err:%s", err.Error())
			}

			return
		}
	}
}

// Notify node to cache blocks
func (c *CacheTask) cacheCarfile2Node() (api.CacheCarfileResult, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cNode := c.carfileRecord.nodeManager.GetCandidateNode(c.deviceID)
	if cNode != nil {
		return cNode.GetAPI().CacheCarfile(ctx, c.carfileRecord.carfileCid, c.carfileRecord.rootCacheDowloadInfos)
	}

	eNode := c.carfileRecord.nodeManager.GetEdgeNode(c.deviceID)
	if eNode != nil {
		return eNode.GetAPI().CacheCarfile(ctx, c.carfileRecord.carfileCid, c.carfileRecord.rootCacheDowloadInfos)
	}

	return api.CacheCarfileResult{}, xerrors.Errorf("not found node:%s", c.deviceID)
}

func (c *CacheTask) blockCacheResult(info *api.CacheResultInfo) error {
	c.doneBlocks = info.DoneBlocks
	c.doneSize = info.DoneSize

	if !c.carfileRecord.rootCacheExists() {
		c.carfileRecord.totalSize = info.TotalSize
		c.carfileRecord.totalBlocks = info.TotalBlock
	}

	log.Infof("blockCacheResult :%s , %d , %s , %d", c.deviceID, info.Status, info.CarfileHash, info.DoneBlocks)

	if info.Status == api.CacheStatusSuccess || info.Status == api.CacheStatusFail {
		//update node dick
		node := c.carfileRecord.nodeManager.GetNode(c.deviceID)
		if node != nil {
			node.DiskUsage = info.DiskUsage
			node.IncrCurCacheCount(-1)
		}

		return c.endCache(info.Status)
	}

	// if err := c.carfileRecord.saveCarfileCacheInfo(c); err != nil {
	// 	return err
	// }

	// update cache task timeout
	return cache.GetDB().UpdateNodeCacheingExpireTime(c.carfileHash, c.deviceID, nodeCacheTimeout)
}

func (c *CacheTask) startCache() error {
	c.executeCount++

	err := cache.GetDB().SetCacheTaskStart(c.carfileHash, c.deviceID, nodeCacheTimeout)
	if err != nil {
		return xerrors.Errorf("startCache %s , SetCacheTaskStart err:%s", c.carfileHash, err.Error())
	}

	go c.startTimeoutTimer()

	// send to node
	result, err := c.cacheCarfile2Node()
	if err != nil {
		return xerrors.Errorf("startCache deviceID:%s, err:%s", c.deviceID, err.Error())
	}

	c.status = api.CacheStatusCreate

	node := c.carfileRecord.nodeManager.GetNode(c.deviceID)
	if node != nil {
		node.DiskUsage = result.DiskUsage
		node.SetCurCacheCount(result.WaitCacheCarfileNum + 1)
	}

	return nil
}

func (c *CacheTask) endCache(status api.CacheStatus) (err error) {
	c.status = status
	c.reliability = c.calculateReliability()

	size := int64(0)
	blocks := 0
	if status == api.CacheStatusSuccess {
		size = c.carfileRecord.totalSize
		blocks = c.carfileRecord.totalBlocks
	}

	err = cache.GetDB().SetCacheTaskEnd(c.carfileHash, c.deviceID, size, blocks)
	if err != nil {
		return xerrors.Errorf("endCache %s , SetCacheTaskEnd err:%s", c.carfileHash, err.Error())
	}

	return c.carfileRecord.cacheDone(c)
}

func (c *CacheTask) calculateReliability() int {
	// TODO To be perfected
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
func (c *CacheTask) GetDoneSize() int64 {
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
