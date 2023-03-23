package caching

import (
	"time"

	"github.com/linguohua/titan/api/types"
)

// CarfileHash is an identifier for a carfile.
type CarfileHash string

func (c CarfileHash) String() string {
	return string(c)
}

// NodeCacheResultInfo node cache carfile result
type NodeCacheResultInfo struct {
	Status             int64
	CarfileBlocksCount int64
	CarfileSize        int64
	NodeID             string
	IsCandidate        bool
}

// CarfileCacheInfo carfile cache info
type CarfileCacheInfo struct {
	ID                string
	State             CarfileState
	CarfileHash       CarfileHash
	CarfileCID        string
	ServerID          string
	Size              int64
	Blocks            int64
	EdgeReplicas      int64
	CandidateReplicas int64
	CreatedAt         int64
	Expiration        int64

	EdgeReplicaSucceeds      []string
	EdgeReplicaFailures      []string
	CandidateReplicaSucceeds []string
	CandidateReplicaFailures []string

	RetryCount int64
}

// ToCarfileRecordInfo types.CarfileRecordInfo
func (state *CarfileCacheInfo) ToCarfileRecordInfo() *types.CarfileRecordInfo {
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

func carfileCacheInfoFrom(info *types.CarfileRecordInfo) *CarfileCacheInfo {
	cInfo := &CarfileCacheInfo{
		CarfileCID:        info.CarfileCID,
		State:             CarfileState(info.State),
		CarfileHash:       CarfileHash(info.CarfileHash),
		EdgeReplicas:      info.NeedEdgeReplica,
		Size:              info.TotalSize,
		Blocks:            info.TotalBlocks,
		CandidateReplicas: info.NeedCandidateReplicas,
		Expiration:        info.Expiration.Unix(),
	}

	for _, r := range info.ReplicaInfos {
		if r.IsCandidate {
			cInfo.CandidateReplicaSucceeds = append(cInfo.CandidateReplicaSucceeds, r.NodeID)
		} else {
			cInfo.EdgeReplicaSucceeds = append(cInfo.EdgeReplicaSucceeds, r.NodeID)
		}
	}

	return cInfo
}
