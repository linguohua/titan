package carfile

import (
	"context"
	"time"

	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/node/scheduler/node"
	"golang.org/x/xerrors"
)

// Replica Replica
type Replica struct {
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

func (cr *CarfileRecord) newReplica(carfileRecord *CarfileRecord, deviceID string, isCandidate bool) (*Replica, error) {
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

	err := cr.nodeManager.CarfileDB.CreateCarfileReplicaInfo(
		&api.ReplicaInfo{
			ID:          cache.id,
			CarfileHash: cache.carfileHash,
			DeviceID:    cache.deviceID,
			Status:      cache.status,
			IsCandidate: cache.isCandidate,
		})
	return cache, err
}

func (ra *Replica) isTimeout() bool {
	// check redis
	expiration, err := ra.nodeManager.CarfileCache.GetNodeCacheTimeoutTime(ra.deviceID)
	if err != nil {
		log.Errorf("GetNodeCacheTimeoutTime err:%s", err.Error())
		return false
	}

	return expiration <= 0
}

func (ra *Replica) startTimeoutTimer() {
	if ra.timeoutTicker != nil {
		return
	}

	ra.timeoutTicker = time.NewTicker(time.Duration(checkCacheTimeoutInterval) * time.Second)
	defer func() {
		ra.timeoutTicker.Stop()
		ra.timeoutTicker = nil
	}()

	for {
		<-ra.timeoutTicker.C
		if ra.status != api.CacheStatusDownloading {
			return
		}

		if !ra.isTimeout() {
			continue
		}

		info := &api.CacheResultInfo{
			Status:         api.CacheStatusFailed,
			DoneSize:       ra.doneSize,
			DoneBlockCount: ra.doneBlocks,
			Msg:            "timeout",
		}

		// task is timeout
		err := ra.carfileRecord.carfileCacheResult(ra.deviceID, info)
		if err != nil {
			log.Errorf("carfileCacheResult err:%s", err.Error())
		}

		return
	}
}

func (ra *Replica) updateInfo() error {
	// update cache info to db
	cInfo := &api.ReplicaInfo{
		ID:      ra.id,
		Status:  ra.status,
		EndTime: time.Now(),
	}

	return ra.nodeManager.CarfileDB.UpdateCarfileReplicaInfo(cInfo)
}

// Notify node to cache carfile
func (ra *Replica) cacheCarfile() (err error) {
	ra.status = api.CacheStatusDownloading

	deviceID := ra.deviceID
	var result *api.CacheCarfileResult
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

	defer func() {
		cancel()

		if result != nil {
			go ra.startTimeoutTimer()

			// update node info
			node := ra.nodeManager.GetNode(deviceID)
			if node != nil {
				node.SetCurCacheCount(result.WaitCacheCarfileNum + 1)
			}
		} else {
			ra.status = api.CacheStatusFailed
		}
	}()

	cNode := ra.nodeManager.GetCandidateNode(deviceID)
	if cNode != nil {
		result, err = cNode.API().CacheCarfile(ctx, ra.carfileRecord.carfileCid, ra.carfileRecord.downloadSources)
		return
	}

	eNode := ra.nodeManager.GetEdgeNode(deviceID)
	if eNode != nil {
		result, err = eNode.API().CacheCarfile(ctx, ra.carfileRecord.carfileCid, ra.carfileRecord.downloadSources)
		return
	}

	err = xerrors.Errorf("not found node:%s", deviceID)
	return
}
