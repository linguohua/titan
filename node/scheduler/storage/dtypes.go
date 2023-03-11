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

type CacheResultInfo struct {
	Status            int64
	CarfileBlockCount int64
	CarfileSize       int64
	NodeID            string
	IsCandidate       bool
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

	SucceedEdgeReplicas      int64
	SucceedCandidateReplicas int64

	FailedEdgeReplicas      int64
	FailedCandidateReplicas int64

	retryCount int64
}

func (state *CarfileInfo) toCarfileRecordInfo() *types.CarfileRecordInfo {
	return &types.CarfileRecordInfo{
		CarfileCID:            state.CarfileCID,
		CarfileHash:           state.CarfileHash.String(),
		NeedEdgeReplica:       state.EdgeReplicas,
		TotalSize:             state.Size,
		TotalBlocks:           state.Blocks,
		State:                 state.State.String(),
		NeedCandidateReplicas: state.CandidateReplicas,
		Expiration:            time.Unix(state.Expiration, 0),
	}
}

func carfileInfoFrom(info *types.CarfileRecordInfo) *CarfileInfo {
	return &CarfileInfo{
		CarfileCID:               info.CarfileCID,
		State:                    CarfileState(info.State),
		CarfileHash:              CarfileHash(info.CarfileHash),
		EdgeReplicas:             info.NeedEdgeReplica,
		Size:                     info.TotalSize,
		Blocks:                   info.TotalBlocks,
		CandidateReplicas:        info.NeedCandidateReplicas,
		Expiration:               info.Expiration.Unix(),
		SucceedEdgeReplicas:      info.SucceedEdgeReplicas,
		SucceedCandidateReplicas: info.SucceedCandidateReplicas,
	}
}
