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
