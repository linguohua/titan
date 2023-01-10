package data

import (
	"context"
	"time"

	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/node/scheduler/db/cache"
	"github.com/linguohua/titan/node/scheduler/db/persistent"
	"golang.org/x/xerrors"
)

// If the node disk size is greater than this value, caching will not continue
const diskUsageMax = 90

// CacheTask CacheTask
type CacheTask struct {
	data *CarfileRecord

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
		data:        data,
		reliability: 0,
		status:      api.CacheStatusCreate,
		carfileHash: data.carfileHash,
		isRootCache: isRootCache,
		expiredTime: data.expiredTime,
		deviceID:    deviceID,
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
func (c *CacheTask) sendBlocksToNode() error {
	//TODO new api
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cNode := c.data.nodeManager.GetCandidateNode(c.deviceID)
	if cNode != nil {
		result, err := cNode.GetAPI().CacheCarfile(ctx, c.data.carfileCid, c.data.source)
		if err != nil {
			log.Errorf("sendBlocksToNode %s, CacheCarfile err:%s", c.deviceID, err.Error())
		} else {
			c.data.nodeManager.UpdateNodeDiskUsage(c.deviceID, result.DiskUsage)
		}
		return err
	}

	eNode := c.data.nodeManager.GetEdgeNode(c.deviceID)
	if eNode != nil {
		result, err := eNode.GetAPI().CacheCarfile(ctx, c.data.carfileCid, c.data.source)
		if err != nil {
			log.Errorf("sendBlocksToNode %s, CacheCarfile err:%s", c.deviceID, err.Error())
		} else {
			c.data.nodeManager.UpdateNodeDiskUsage(c.deviceID, result.DiskUsage)
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
		c.data.nodeManager.UpdateNodeDiskUsage(c.deviceID, info.DiskUsage)

		c.endCache(info.Status)
	} else {
		// update data task timeout
		err := cache.GetDB().SetRunningDataTask(c.carfileHash, c.deviceID, nodeCacheResultInterval)
		if err != nil {
			log.Panicf("blockCacheResult %s , SetRunningDataTask err:%s", c.deviceID, err.Error())
		}
	}

	return nil
}

// update node block info in redis
func (c *CacheTask) updateNodeBlockInfo(deviceID, fromDeviceID string, blockSize int) {
	fromID := ""

	node := c.data.nodeManager.GetCandidateNode(fromDeviceID)
	if node != nil {
		fromID = fromDeviceID
	}

	err := cache.GetDB().UpdateNodeCacheBlockInfo(deviceID, fromID, blockSize)
	if err != nil {
		log.Errorf("UpdateNodeCacheBlockInfo err:%s", err.Error())
	}
}

func (c *CacheTask) startCache() error {
	c.cacheCount++
	//TODO send to node
	err := c.sendBlocksToNode()
	if err != nil {
		return xerrors.Errorf("startCache deviceID:%s, err:%s", c.deviceID, err.Error())
	}

	err = cache.GetDB().SetDataTaskToRunningList(c.carfileHash, c.deviceID, nodeCacheResultInterval)
	if err != nil {
		return xerrors.Errorf("startCache %s , SetDataTaskToRunningList err:%s", c.carfileHash, err.Error())
	}

	err = saveEvent(c.data.carfileCid, c.deviceID, "", "", eventTypeDoCacheTaskStart)
	if err != nil {
		return xerrors.Errorf("startCache %s , saveEvent err:%s", c.carfileHash, err.Error())
	}

	return nil
}

func (c *CacheTask) endCache(status api.CacheStatus) (err error) {
	saveEvent(c.data.carfileCid, c.deviceID, "", "", eventTypeDoCacheTaskEnd)

	err = cache.GetDB().RemoveRunningDataTask(c.carfileHash, c.deviceID)
	if err != nil {
		err = xerrors.Errorf("endCache RemoveRunningDataTask err: %s", err.Error())
		return
	}

	defer func() {
		c.data.cacheEnd(c)
	}()

	c.status = status
	c.reliability = c.calculateReliability("")

	return
}

func (c *CacheTask) removeCache() error {
	err := cache.GetDB().RemoveRunningDataTask(c.carfileHash, c.deviceID)
	if err != nil {
		return xerrors.Errorf("removeCache RemoveRunningDataTask err: %s", err.Error())
	}

	go c.notifyNodeRemoveBlocks(c.deviceID, []string{c.carfileHash})

	values := map[string]int64{}
	values[c.deviceID] = -int64(c.doneBlocks)
	// update node block count
	err = cache.GetDB().IncrByDevicesInfo(cache.BlockCountField, values)
	if err != nil {
		log.Errorf("IncrByDevicesInfo err:%s ", err.Error())
	}

	if c.status == api.CacheStatusSuccess {
		c.data.reliability -= c.reliability
		err = cache.GetDB().IncrByBaseInfo(cache.CarFileCountField, -1)
		if err != nil {
			log.Errorf("removeCache IncrByBaseInfo err:%s", err.Error())
		}
	}

	isDelete := true
	c.data.CacheMap.Range(func(key, value interface{}) bool {
		if value != nil {
			ca := value.(*CacheTask)
			if ca != nil && c.deviceID != ca.deviceID {
				isDelete = false
			}
		}

		return true
	})

	// delete cache and update data info
	err = persistent.GetDB().RemoveCacheAndUpdateData(c.deviceID, c.carfileHash, isDelete, c.data.reliability)

	c.data.CacheMap.Delete(c.deviceID)
	c = nil

	return err
}

// Notify nodes to delete blocks
func (c *CacheTask) notifyNodeRemoveBlocks(deviceID string, cids []string) {
	// ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	// defer cancel()

	edge := c.data.nodeManager.GetEdgeNode(deviceID)
	if edge != nil {
		_, err := edge.GetAPI().DeleteBlocks(context.Background(), cids)
		if err != nil {
			log.Errorf("notifyNodeRemoveBlocks DeleteBlocks err:%s", err.Error())
		}

		return
	}

	candidate := c.data.nodeManager.GetCandidateNode(deviceID)
	if candidate != nil {
		_, err := candidate.GetAPI().DeleteBlocks(context.Background(), cids)
		if err != nil {
			log.Errorf("notifyNodeRemoveBlocks DeleteBlocks err:%s", err.Error())
		}

		return
	}
}

func (c *CacheTask) calculateReliability(deviceID string) int {
	// TODO To be perfected
	if deviceID != "" {
		return 1
	}

	if c.status == api.CacheStatusSuccess {
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
