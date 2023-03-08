package storage

import (
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
	Status            int64
	CarfileBlockCount int64
	CarfileSize       int64
	NodeID            string
	IsCandidate       bool
	Source            *types.DownloadSource
}

type CompletedValue struct{}

type CarfileInfo struct {
	ID          string
	State       CarfileState `db:"state"`
	CarfileHash CarfileID    `db:"carfile_hash"`
	CarfileCID  string       `db:"carfile_cid"`
	Replicas    int64        `db:"replicas"` // edge replica
	ServerID    string       `db:"server_id"`
	Size        int64        `db:"size"`
	Blocks      int64        `db:"blocks"`
	CreatedAt   int64        `db:"created_at"`
	Expiration  int64        `db:"expiration"`

	CandidateReplicas int64 // seed + other candidate replica

	Log                 []Log
	CandidateStoreFails int64
	EdgeStoreFails      int64

	CompletedEdgeReplicas      map[string]*CompletedValue
	CompletedCandidateReplicas map[string]*CompletedValue
	DownloadSources            []*types.DownloadSource

	LastResultInfo *NodeCacheResult
}
