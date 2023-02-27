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
	nodeManager   *node.Manager

	id          string
	deviceID    string
	carfileHash string
	status      api.CacheStatus
	doneSize    int64
	doneBlocks  int
	isCandidate bool
	createTime  time.Time
	endTime     time.Time

	timeoutTicker *time.Ticker
}

func newCacheTask(carfileRecord *CarfileRecord, deviceID string, isCandidate bool) (*CacheTask, error) {
	cache := &CacheTask{
		carfileRecord: carfileRecord,
		nodeManager:   carfileRecord.nodeManager,
		status:        api.CacheStatusRunning,
		carfileHash:   carfileRecord.carfileHash,
		isCandidate:   isCandidate,
		deviceID:      deviceID,
		id:            cacheTaskID(carfileRecord.carfileHash, deviceID),
		createTime:    time.Now(),
	}

	err := persistent.CreateCarfileReplicaInfo(
		&api.CarfileReplicaInfo{
			ID:          cache.id,
			CarfileHash: cache.carfileHash,
			DeviceID:    cache.deviceID,
			Status:      cache.status,
			IsCandidate: cache.isCandidate,
		})
	return cache, err
}

func (c *CacheTask) isTimeout() bool {
	// check redis
	expiration, err := cache.GetDB().GetNodeCacheTimeoutTime(c.deviceID)
	if err != nil {
		log.Errorf("GetNodeCacheTimeoutTime err:%s", err.Error())
		return false
	}

	return expiration <= 0
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
			continue
		}

		info := &api.CacheResultInfo{
			Status:         api.CacheStatusFailed,
			DoneSize:       c.doneSize,
			DoneBlockCount: c.doneBlocks,
			Msg:            "timeout",
		}

		// task is timeout
		err := c.carfileRecord.carfileCacheResult(c.deviceID, info)
		if err != nil {
			log.Errorf("carfileCacheResult err:%s", err.Error())
		}

		return
	}
}

func (c *CacheTask) startTask() (err error) {
	go c.startTimeoutTimer()

	c.status = api.CacheStatusRunning

	// send to node
	err = c.cacheCarfile2Node()
	if err != nil {
		c.status = api.CacheStatusFailed
	}
	return err
}

func (c *CacheTask) updateCacheTaskInfo() error {
	// update cache info to db
	cInfo := &api.CarfileReplicaInfo{
		ID:          c.id,
		CarfileHash: c.carfileHash,
		DeviceID:    c.deviceID,
		Status:      c.status,
		DoneSize:    c.doneSize,
		DoneBlocks:  c.doneBlocks,
		EndTime:     time.Now(),
	}

	return persistent.UpdateCarfileReplicaInfo(cInfo)
}

// Notify node to cache blocks
func (c *CacheTask) cacheCarfile2Node() (err error) {
	deviceID := c.deviceID
	var result *api.CacheCarfileResult
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

	defer func() {
		cancel()

		if result != nil {
			// update node info
			node := c.nodeManager.GetNode(deviceID)
			if node != nil {
				node.SetCurCacheCount(result.WaitCacheCarfileNum + 1)
			}
		}
	}()

	cNode := c.nodeManager.GetCandidateNode(deviceID)
	if cNode != nil {
		result, err = cNode.GetAPI().CacheCarfile(ctx, c.carfileRecord.carfileCid, c.carfileRecord.dowloadSources)
		return
	}

	eNode := c.nodeManager.GetEdgeNode(deviceID)
	if eNode != nil {
		result, err = eNode.GetAPI().CacheCarfile(ctx, c.carfileRecord.carfileCid, c.carfileRecord.dowloadSources)
		return
	}

	err = xerrors.Errorf("not found node:%s", deviceID)
	return
}
