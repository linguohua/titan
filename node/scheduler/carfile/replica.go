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

// Replica Replica
type Replica struct {
	carfileRecord *Record
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

func newReplica(carfileRecord *Record, deviceID string, isCandidate bool) (*Replica, error) {
	cache := &Replica{
		carfileRecord: carfileRecord,
		nodeManager:   carfileRecord.nodeManager,
		status:        api.CacheStatusDownloading,
		carfileHash:   carfileRecord.carfileHash,
		isCandidate:   isCandidate,
		deviceID:      deviceID,
		id:            replicaID(carfileRecord.carfileHash, deviceID),
		createTime:    time.Now(),
	}

	err := persistent.CreateCarfileReplicaInfo(
		&api.ReplicaInfo{
			ID:          cache.id,
			CarfileHash: cache.carfileHash,
			DeviceID:    cache.deviceID,
			Status:      cache.status,
			IsCandidate: cache.isCandidate,
		})
	return cache, err
}

func (c *Replica) isTimeout() bool {
	// check redis
	expiration, err := cache.GetNodeCacheTimeoutTime(c.deviceID)
	if err != nil {
		log.Errorf("GetNodeCacheTimeoutTime err:%s", err.Error())
		return false
	}

	return expiration <= 0
}

func (c *Replica) startTimeoutTimer() {
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
		if c.status != api.CacheStatusDownloading {
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

func (c *Replica) startTask() (err error) {
	go c.startTimeoutTimer()

	c.status = api.CacheStatusDownloading

	// send to node
	err = c.cacheCarfile2Node()
	if err != nil {
		c.status = api.CacheStatusFailed
	}
	return err
}

func (c *Replica) updateReplicaInfo() error {
	// update cache info to db
	cInfo := &api.ReplicaInfo{
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
func (c *Replica) cacheCarfile2Node() (err error) {
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
		result, err = cNode.GetAPI().CacheCarfile(ctx, c.carfileRecord.carfileCid, c.carfileRecord.downloadSources)
		return
	}

	eNode := c.nodeManager.GetEdgeNode(deviceID)
	if eNode != nil {
		result, err = eNode.GetAPI().CacheCarfile(ctx, c.carfileRecord.carfileCid, c.carfileRecord.downloadSources)
		return
	}

	err = xerrors.Errorf("not found node:%s", deviceID)
	return
}
