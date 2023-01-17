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

	deviceID     string
	carfileHash  string
	status       api.CacheStatus
	reliability  int
	doneSize     int64
	doneBlocks   int
	isRootCache  bool
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
		deviceID:      deviceID,
	}

	err := persistent.GetDB().CreateCacheInfo(
		&api.CacheTaskInfo{
			CarfileHash: cache.carfileHash,
			DeviceID:    cache.deviceID,
			Status:      cache.status,
			RootCache:   cache.isRootCache,
		})
	if err != nil {
		return nil, err
	}

	return cache, err
}

func (c *CacheTask) isTimeout() bool {
	// check redis
	exist, err := cache.GetDB().IsNodeCaching(c.deviceID)
	if err != nil {
		log.Errorf("NodeIsCaching err:%s", err.Error())
		return false
	}

	log.Infof("check timeout device:%s , exist:%v", c.deviceID, exist)
	if exist {
		return false
	}

	return true
}

func (c *CacheTask) startTimeoutTimer() {
	if c.timeoutTicker != nil {
		return
	}

	c.timeoutTicker = time.NewTicker(time.Duration(checkCacheTimeoutInterval) * time.Second)
	defer func() {
		c.timeoutTicker.Stop()
		c.timeoutTicker = nil
	}()

	for {
		<-c.timeoutTicker.C
		if c.status != api.CacheStatusRunning {
			return
		}

		if !c.isTimeout() {
			break
		}

		// cache is timeout
		err := c.endCache(api.CacheStatusTimeout, 0)
		if err != nil {
			log.Errorf("endCache err:%s", err.Error())
		}

		return
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

func (c *CacheTask) carfileCacheResult(info *api.CacheResultInfo) error {
	c.doneBlocks = info.DoneBlocks
	c.doneSize = info.DoneSize

	if !c.carfileRecord.rootCacheExists() {
		c.carfileRecord.totalSize = info.TotalSize
		c.carfileRecord.totalBlocks = info.TotalBlock
	}

	log.Infof("carfileCacheResult :%s , %d , %s , %d", c.deviceID, info.Status, info.CarfileHash, info.DoneBlocks)

	if info.Status == api.CacheStatusSuccess || info.Status == api.CacheStatusFail {
		return c.endCache(info.Status, info.DiskUsage)
	}

	// update cache task timeout
	err := cache.GetDB().UpdateNodeCacheingExpireTime(c.carfileHash, c.deviceID, nodeCacheTimeout)
	if err != nil {
		return err
	}

	return c.carfileRecord.saveCarfileCacheInfo(c)
}

func (c *CacheTask) startCache() error {
	c.executeCount++

	err := cache.GetDB().CacheTaskStart(c.carfileHash, c.deviceID, nodeCacheTimeout)
	if err != nil {
		return xerrors.Errorf("startCache %s , CacheTaskStart err:%s", c.carfileHash, err.Error())
	}

	go c.startTimeoutTimer()

	defer func() {
		if err != nil {
			c.status = api.CacheStatusFail
			// cache not start
			err = cache.GetDB().CacheTaskEnd(c.carfileHash, c.deviceID, nil)
			if err != nil {
				log.Errorf("startCache %s , CacheTaskEnd err:%s", c.carfileHash, err.Error())
			}
		}
	}()

	c.status = api.CacheStatusRunning
	// send to node
	result, err := c.cacheCarfile2Node()
	if err != nil {
		return xerrors.Errorf("startCache deviceID:%s, err:%s", c.deviceID, err.Error())
	}

	node := c.carfileRecord.nodeManager.GetNode(c.deviceID)
	if node != nil {
		node.DiskUsage = result.DiskUsage
		node.SetCurCacheCount(result.WaitCacheCarfileNum + 1)
	}

	return nil
}

func (c *CacheTask) endCache(status api.CacheStatus, diskUsage float64) (err error) {
	c.status = status
	c.reliability = c.calculateReliability()

	// update node info
	var nodeInfo *cache.NodeCacheInfo
	node := c.carfileRecord.nodeManager.GetNode(c.deviceID)
	if node != nil {
		node.IncrCurCacheCount(-1)

		if diskUsage > 0 {
			node.DiskUsage = diskUsage
		}

		node.TotalDownload += float64(c.doneSize)
		node.DownloadCount += c.doneBlocks
		node.BlockCount += c.doneBlocks

		nodeInfo = &cache.NodeCacheInfo{}
		nodeInfo.BlockCount = node.BlockCount
		nodeInfo.DiskUsage = node.DiskUsage
		nodeInfo.TotalDownload = node.TotalDownload
		nodeInfo.IsSuccess = c.status == api.CacheStatusSuccess
	}

	err = cache.GetDB().CacheTaskEnd(c.carfileHash, c.deviceID, nodeInfo)
	if err != nil {
		return xerrors.Errorf("endCache %s , CacheTaskEnd err:%s", c.carfileHash, err.Error())
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
