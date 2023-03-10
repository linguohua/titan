package storage

import (
	"context"
	"time"

	"github.com/linguohua/titan/api/types"
	"github.com/linguohua/titan/node/scheduler/node"
	"golang.org/x/xerrors"
)

// Replica Replica
type Replica struct {
	carfileRecord *CarfileRecord
	nodeManager   *node.Manager

	id          string
	nodeID      string
	carfileHash string
	status      types.CacheStatus
	doneSize    int64
	doneBlocks  int
	isCandidate bool
	createTime  time.Time
	endTime     time.Time

	timeoutTicker *time.Ticker
	countDown     int
}

func (cr *CarfileRecord) newReplica(carfileRecord *CarfileRecord, nodeID string, isCandidate bool) (*Replica, error) {
	cache := &Replica{
		carfileRecord: carfileRecord,
		nodeManager:   carfileRecord.nodeManager,
		status:        types.CacheStatusDownloading,
		carfileHash:   carfileRecord.carfileHash,
		isCandidate:   isCandidate,
		nodeID:        nodeID,
		id:            replicaID(carfileRecord.carfileHash, nodeID),
		createTime:    time.Now(),
	}

	err := cr.nodeManager.CarfileDB.CreateCarfileReplicaInfo(
		&types.ReplicaInfo{
			ID:          cache.id,
			CarfileHash: cache.carfileHash,
			NodeID:      cache.nodeID,
			Status:      cache.status,
			IsCandidate: cache.isCandidate,
		})
	return cache, err
}

func (ra *Replica) startTimeoutTimer() {
	if ra.timeoutTicker != nil {
		return
	}

	ra.timeoutTicker = time.NewTicker(time.Duration(nodoCachingKeepalive) * time.Second)
	defer func() {
		ra.timeoutTicker.Stop()
		ra.timeoutTicker = nil
	}()

	for {
		<-ra.timeoutTicker.C
		if ra.status != types.CacheStatusDownloading {
			return
		}

		if ra.countDown > 0 {
			ra.countDown -= nodoCachingKeepalive
			continue
		}

		info := &types.CacheResult{
			Status:         types.CacheStatusFailed,
			DoneSize:       ra.doneSize,
			DoneBlockCount: ra.doneBlocks,
			Msg:            "timeout",
		}

		// task is timeout
		err := ra.carfileRecord.carfileCacheResult(ra.nodeID, info)
		if err != nil {
			log.Errorf("carfileCacheResult err:%s", err.Error())
		}

		return
	}
}

func (ra *Replica) updateInfo() error {
	// update cache info to db
	// cInfo := &types.ReplicaInfo{
	// 	ID:      ra.id,
	// 	Status:  ra.status,
	// 	EndTime: time.Now(),
	// }

	// return ra.nodeManager.CarfileDB.UpdateCarfileReplicaInfo(cInfo)
	return nil
}

// Notify node to cache storage
func (ra *Replica) cacheCarfile(cDown int) (err error) {
	ra.status = types.CacheStatusDownloading

	nodeID := ra.nodeID
	var result *types.CacheCarfileResult
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

	defer func() {
		cancel()

		if result != nil {
			ra.countDown = cDown
			go ra.startTimeoutTimer()

			// update node info
			node := ra.nodeManager.GetNode(nodeID)
			if node != nil {
				node.SetCurCacheCount(result.WaitCacheCarfileNum + 1)
			}
		} else {
			ra.status = types.CacheStatusFailed
		}
	}()

	cNode := ra.nodeManager.GetCandidateNode(nodeID)
	if cNode != nil {
		result, err = cNode.API().CacheCarfile(ctx, ra.carfileRecord.carfileCid, ra.carfileRecord.downloadSources)
		return
	}

	eNode := ra.nodeManager.GetEdgeNode(nodeID)
	if eNode != nil {
		result, err = eNode.API().CacheCarfile(ctx, ra.carfileRecord.carfileCid, ra.carfileRecord.downloadSources)
		return
	}

	err = xerrors.Errorf("not found node:%s", nodeID)
	return
}
