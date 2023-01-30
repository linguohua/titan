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

	id               string
	deviceID         string
	carfileHash      string
	status           api.CacheStatus
	reliability      int
	doneSize         int64
	doneBlocks       int
	isCandidateCache bool

	timeoutTicker *time.Ticker
}

func newCache(carfileRecord *CarfileRecord, deviceID string, isCandidateCache bool) (*CacheTask, error) {
	cache := &CacheTask{
		carfileRecord:    carfileRecord,
		reliability:      0,
		status:           api.CacheStatusCreate,
		carfileHash:      carfileRecord.carfileHash,
		isCandidateCache: isCandidateCache,
		deviceID:         deviceID,
		id:               carfileRecord.carfileManager.cacheTaskID(carfileRecord.carfileHash, deviceID),
	}

	err := persistent.GetDB().CreateCacheTaskInfo(
		&api.CacheTaskInfo{
			ID:             cache.id,
			CarfileHash:    cache.carfileHash,
			DeviceID:       cache.deviceID,
			Status:         cache.status,
			CandidateCache: cache.isCandidateCache,
		})
	return cache, err
}

func (c *CacheTask) isTimeout() bool {
	// check redis
	exist, err := cache.GetDB().IsNodeCaching(c.deviceID)
	if err != nil {
		log.Errorf("NodeIsCaching err:%s", err.Error())
		return false
	}

	return !exist
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
		err := c.endTask(api.CacheStatusTimeout, 0)
		if err != nil {
			log.Errorf("endCache err:%s", err.Error())
		}

		return
	}
}

// Notify node to cache blocks
func (c *CacheTask) cacheCarfile2Node() (err error) {
	var result *api.CacheCarfileResult
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer func() {
		cancel()

		if result != nil {
			// update node info
			node := c.carfileRecord.nodeManager.GetNode(c.deviceID)
			if node != nil {
				// node.DiskUsage = result.DiskUsage
				node.SetCurCacheCount(result.WaitCacheCarfileNum + 1)
			}
		}
	}()

	cNode := c.carfileRecord.nodeManager.GetCandidateNode(c.deviceID)
	if cNode != nil {
		result, err = cNode.GetAPI().CacheCarfile(ctx, c.carfileRecord.carfileCid, c.carfileRecord.dowloadSources)
		return
	}

	eNode := c.carfileRecord.nodeManager.GetEdgeNode(c.deviceID)
	if eNode != nil {
		result, err = eNode.GetAPI().CacheCarfile(ctx, c.carfileRecord.carfileCid, c.carfileRecord.dowloadSources)
		return
	}

	err = xerrors.Errorf("not found node:%s", c.deviceID)
	return
}

func (c *CacheTask) carfileCacheResult(info *api.CacheResultInfo) error {
	c.doneBlocks = info.DoneBlockCount
	c.doneSize = info.DoneSize

	if info.Status == api.CacheStatusSuccess || info.Status == api.CacheStatusFail {
		return c.endTask(info.Status, info.DiskUsage)
	}

	// update cache task timeout
	return cache.GetDB().UpdateNodeCacheingExpireTime(c.carfileHash, c.deviceID, nodeCacheTimeout)
}

func (c *CacheTask) updateCacheTaskInfo() error {
	// update cache info to db
	cInfo := &api.CacheTaskInfo{
		ID:          c.id,
		CarfileHash: c.carfileHash,
		DeviceID:    c.deviceID,
		Status:      c.status,
		DoneSize:    c.doneSize,
		DoneBlocks:  c.doneBlocks,
		Reliability: c.reliability,
		EndTime:     time.Now(),
	}

	return persistent.GetDB().UpdateCacheTaskInfo(cInfo)
}

func (c *CacheTask) startTask() (err error) {
	defer func() {
		if err != nil {
			c.status = api.CacheStatusFail
		}
	}()

	go c.startTimeoutTimer()

	c.status = api.CacheStatusRunning

	// send to node
	err = c.cacheCarfile2Node()
	return err
}

func (c *CacheTask) endTask(status api.CacheStatus, diskUsage float64) (err error) {
	c.status = status
	c.reliability = c.calculateReliability()

	// update node info
	node := c.carfileRecord.nodeManager.GetNode(c.deviceID)
	if node != nil {
		node.IncrCurCacheCount(-1)
	}

	err = c.updateCacheTaskInfo()
	if err != nil {
		log.Errorf("endCache %s , updateCacheTaskInfo err:%s", c.carfileHash, err.Error())
	}

	cachesDone, err := cache.GetDB().CacheTasksEnd(c.carfileHash, []string{c.deviceID})
	if err != nil {
		return xerrors.Errorf("endCache %s , CacheTasksEnd err:%s", c.carfileHash, err.Error())
	}

	return c.carfileRecord.cacheDone(c, cachesDone)
}

func (c *CacheTask) calculateReliability() int {
	// TODO To be perfected
	if !c.isCandidateCache && c.status == api.CacheStatusSuccess {
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
	return c.isCandidateCache
}
