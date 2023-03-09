package storage

import (
	"time"

	"github.com/linguohua/titan/api/types"
)

// CarfileHash is an identifier for a carfile.
type CarfileHash string

func (c CarfileHash) String() string {
	return string(c)
}

type Log struct {
	Timestamp uint64
	Trace     string // for errors

	Message string

	// additional data (Event info)
	Kind string
}

type DownloadSource struct {
	CandidateURL   string
	CandidateToken string
}

type NodeCacheResult struct {
	Status            int64
	CarfileBlockCount int64
	CarfileSize       int64
	NodeID            string
	IsCandidate       bool
	Source            *DownloadSource
}

type CompletedValue struct{}

type CarfileInfo struct {
	ID                string
	State             CarfileState
	CarfileHash       CarfileHash
	CarfileCID        string
	EdgeReplicas      int64
	ServerID          string
	Size              int64
	Blocks            int64
	CandidateReplicas int64
	CreatedAt         int64
	Expiration        int64

	// DownloadSources            []*types.DownloadSource
	// CompletedEdgeReplicas      map[string]*CompletedValue
	// CompletedCandidateReplicas map[string]*CompletedValue
}

func (state *CarfileInfo) toCarfileRecordInfo() *types.CarfileRecordInfo {
	return &types.CarfileRecordInfo{
		CarfileCid:            state.CarfileCID,
		CarfileHash:           state.CarfileHash.String(),
		NeedEdgeReplica:       int(state.EdgeReplicas),
		TotalSize:             state.Size,
		TotalBlocks:           int(state.Blocks),
		State:                 string(state.State),
		NeedCandidateReplicas: int(state.CandidateReplicas),
		Expiration:            time.Unix(state.Expiration, 0),
	}
}

func From(info *types.CarfileRecordInfo) *CarfileInfo {
	return &CarfileInfo{
		CarfileCID:        info.CarfileCid,
		State:             CarfileState(info.State),
		CarfileHash:       CarfileHash(info.CarfileHash),
		EdgeReplicas:      int64(info.NeedEdgeReplica),
		Size:              info.TotalSize,
		CandidateReplicas: int64(info.NeedCandidateReplicas),
		Expiration:        info.Expiration.Unix(),
	}
}
