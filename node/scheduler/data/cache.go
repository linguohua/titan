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

// Cache Cache
type Cache struct {
	data *Data

	deviceID    string
	carfileHash string
	status      api.CacheStatus
	reliability int
	doneSize    int
	doneBlocks  int
	totalSize   int
	totalBlocks int
	nodes       int
	isRootCache bool
	expiredTime time.Time
	cacheCount  int
}

func newCache(data *Data, deviceID string, isRootCache bool) (*Cache, error) {
	cache := &Cache{
		data:        data,
		reliability: 0,
		status:      api.CacheStatusCreate,
		carfileHash: data.carfileHash,
		isRootCache: isRootCache,
		expiredTime: data.expiredTime,
		deviceID:    deviceID,
	}

	err := persistent.GetDB().CreateCache(
		&api.CacheInfo{
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
func (c *Cache) sendBlocksToNode() (int64, error) {
	//TODO new api
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cNode := c.data.nodeManager.GetCandidateNode(c.deviceID)
	if cNode != nil {
		// reqDatas := c.Manager.FindDownloadinfoForBlocks(blocks, c.carfileHash, c.cacheID)
		nodeCacheStat, err := cNode.GetAPI().CacheBlocks(ctx, nil)
		if err != nil {
			log.Errorf("sendBlocksToNode %s, CacheBlocks err:%s", c.deviceID, err.Error())
		} else {
			cNode.UpdateCacheStat(&nodeCacheStat)
		}
		return cNode.GetCacheNextTimeoutTimeStamp(), err
	}

	eNode := c.data.nodeManager.GetEdgeNode(c.deviceID)
	if eNode != nil {
		// reqDatas := c.Manager.FindDownloadinfoForBlocks(blocks, c.carfileHash, c.cacheID)
		nodeCacheStat, err := eNode.GetAPI().CacheBlocks(ctx, nil)
		if err != nil {
			log.Errorf("sendBlocksToNode %s, CacheBlocks err:%s", c.deviceID, err.Error())
		} else {
			eNode.UpdateCacheStat(&nodeCacheStat)
		}

		return eNode.GetCacheNextTimeoutTimeStamp(), err
	}

	return 0, xerrors.Errorf("not found node:%s", c.deviceID)
}

func (c *Cache) blockCacheResult(info *api.CacheResultInfo) error {
	c.doneBlocks = info.DoneBlocks
	c.doneSize = info.DoneSize

	if info.IsDone {
		//update node dick
		c.data.nodeManager.UpdateNodeDiskUsage(c.deviceID, info.DiskUsage)

		c.endCache(api.CacheStatusSuccess)
	} else {
		// update data task timeout
		c.data.dataManager.updateDataTimeout(c.carfileHash, c.deviceID, int64(info.TimeLeft), 0)
	}

	return nil
}

// update node block info in redis
func (c *Cache) updateNodeBlockInfo(deviceID, fromDeviceID string, blockSize int) {
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

func (c *Cache) startCache() error {
	c.cacheCount++
	//TODO send to node
	needTime, err := c.sendBlocksToNode()
	if err != nil {
		return xerrors.Errorf("startCache deviceID:%s, err:%s", c.deviceID, err.Error())
	}

	err = cache.GetDB().SetDataTaskToRunningList(c.carfileHash, c.deviceID, needTime)
	if err != nil {
		return xerrors.Errorf("startCache %s , SetDataTaskToRunningList err:%s", c.carfileHash, err.Error())
	}

	err = saveEvent(c.data.carfileCid, c.deviceID, "", "", eventTypeDoCacheTaskStart)
	if err != nil {
		return xerrors.Errorf("startCache %s , saveEvent err:%s", c.carfileHash, err.Error())
	}

	return nil
}

func (c *Cache) endCache(status api.CacheStatus) (err error) {
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

func (c *Cache) removeCache() error {
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
			ca := value.(*Cache)
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
func (c *Cache) notifyNodeRemoveBlocks(deviceID string, cids []string) {
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

func (c *Cache) calculateReliability(deviceID string) int {
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
func (c *Cache) GetDeviceID() string {
	return c.deviceID
}

// GetStatus get status
func (c *Cache) GetStatus() api.CacheStatus {
	return c.status
}

// GetDoneSize get done size
func (c *Cache) GetDoneSize() int {
	return c.doneSize
}

// GetDoneBlocks get done blocks
func (c *Cache) GetDoneBlocks() int {
	return c.doneBlocks
}

// GetNodes get nodes
func (c *Cache) GetNodes() int {
	return c.nodes
}

// IsRootCache get is root cache
func (c *Cache) IsRootCache() bool {
	return c.isRootCache
}
