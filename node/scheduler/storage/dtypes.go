package storage

import (
	"time"

	"github.com/linguohua/titan/api/types"
)

// CarfileID is an identifier for a carfile.
type CarfileID string

func (c CarfileID) String() string {
	return string(c)
}

type Log struct {
	Timestamp uint64
	Trace     string // for errors

	Message string

	// additional data (Event info)
	Kind string
}

type NodeCacheResult struct {
	Status            types.CacheStatus
	CarfileBlockCount int
	CarfileSize       int64
	NodeID            string
	IsCandidate       bool
	Source            *types.DownloadSource
}

type CarfileInfo struct {
	ID                string
	State             CarfileState `db:"state"`
	CarfileCID        CarfileID    `db:"carfile_cid"`
	CarfileHash       string       `db:"carfile_hash"`
	Replicas          int          `db:"replicas"` // edge replica
	ServerID          string       `db:"server_id"`
	Size              int64        `db:"size"`
	Blocks            int64        `db:"blocks"`
	CreatedAt         time.Time    `db:"created_at"`
	Expiration        time.Time    `db:"expiration"`
	candidateReplicas int          // seed + other candidate replica

	Log                 []Log
	CandidateStoreFails int
	EdgeStoreFails      int

	completedEdgeReplicas      map[string]struct{}
	completedCandidateReplicas map[string]struct{}
	downloadSources            []*types.DownloadSource

	lastResultInfo *NodeCacheResult
}

func (state *CarfileInfo) toCacheCarfileInfo() *types.CacheCarfileInfo {
	return &types.CacheCarfileInfo{
		CarfileCid:  string(state.CarfileCID),
		CarfileHash: state.CarfileHash,
		Replicas:    state.Replicas,
		ServerID:    state.ServerID,
		Expiration:  state.Expiration,
	}
}

func fromCarfileInfo(info *types.CacheCarfileInfo) *CarfileInfo {
	return &CarfileInfo{
		CarfileCID:  CarfileID(info.CarfileCid),
		CarfileHash: info.CarfileHash,
		Replicas:    info.Replicas,
		ServerID:    info.ServerID,
		Expiration:  info.Expiration,
	}
}
