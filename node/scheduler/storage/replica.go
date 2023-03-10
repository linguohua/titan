package storage

import (
	"time"

	"github.com/linguohua/titan/api/types"
	"github.com/linguohua/titan/node/scheduler/node"
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
	doneBlocks  int64
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
