package caching

import (
	"github.com/linguohua/titan/api/types"
	"golang.org/x/xerrors"
)

type mutator interface {
	apply(state *CarfileCacheInfo)
}

// globalMutator is an event which can apply in every state
type globalMutator interface {
	// applyGlobal applies the event to the state. If if returns true,
	//  event processing should be interrupted
	applyGlobal(state *CarfileCacheInfo) bool
}

// Ignorable Ignorable
type Ignorable interface {
	Ignore()
}

// Global events

// CarfileRestart restart
type CarfileRestart struct{}

func (evt CarfileRestart) applyGlobal(state *CarfileCacheInfo) bool {
	state.RetryCount = 0
	return false
}

// CarfileFatalError Carfile fatal error
type CarfileFatalError struct{ error }

// FormatError Format error
func (evt CarfileFatalError) FormatError(xerrors.Printer) (next error) { return evt.error }

func (evt CarfileFatalError) applyGlobal(state *CarfileCacheInfo) bool {
	log.Errorf("Fatal error on carfile %s: %+v", state.CarfileCID, evt.error)
	return true
}

// CarfileForceState carfile force
type CarfileForceState struct {
	State CarfileState
}

func (evt CarfileForceState) applyGlobal(state *CarfileCacheInfo) bool {
	state.State = evt.State
	return true
}

// CarfileRemove remove
type CarfileRemove struct{}

func (evt CarfileRemove) applyGlobal(state *CarfileCacheInfo) bool {
	*state = CarfileCacheInfo{State: Removing}
	return true
}

// CarfileInfoUpdate update carfile info
type CarfileInfoUpdate struct {
	ResultInfo *NodeCacheResultInfo
}

func (evt CarfileInfoUpdate) applyGlobal(state *CarfileCacheInfo) bool {
	rInfo := evt.ResultInfo
	if rInfo == nil {
		return true
	}

	if state.State == CarfileSeedCaching {
		state.Size = rInfo.CarfileSize
		state.Blocks = rInfo.CarfileBlocksCount
	}

	return true
}

// CacheResult nodes cache result
type CacheResult struct {
	ResultInfo *NodeCacheResultInfo
}

func (evt CacheResult) apply(state *CarfileCacheInfo) {
	rInfo := evt.ResultInfo
	if rInfo == nil {
		return
	}

	if state.State == CarfileSeedCaching {
		state.Size = rInfo.CarfileSize
		state.Blocks = rInfo.CarfileBlocksCount
	}

	nodeID := rInfo.NodeID
	if rInfo.Status == int64(types.CacheStatusSucceeded) {
		if rInfo.IsCandidate {
			if !exist(state.CandidateReplicaSucceeds, nodeID) {
				state.CandidateReplicaSucceeds = append(state.CandidateReplicaSucceeds, nodeID)
			}
		} else {
			if !exist(state.EdgeReplicaSucceeds, nodeID) {
				state.EdgeReplicaSucceeds = append(state.EdgeReplicaSucceeds, nodeID)
			}
		}
	} else if rInfo.Status == int64(types.CacheStatusFailed) {
		if rInfo.IsCandidate {
			if !exist(state.CandidateReplicaFailures, nodeID) {
				state.CandidateReplicaFailures = append(state.CandidateReplicaFailures, nodeID)
			}
		} else {
			if !exist(state.EdgeReplicaFailures, nodeID) {
				state.EdgeReplicaFailures = append(state.EdgeReplicaFailures, nodeID)
			}
		}
	}
}

func exist(list []string, c string) bool {
	for _, str := range list {
		if str == c {
			return true
		}
	}

	return false
}

// CacheRequestSent request nodes cache carfile
type CacheRequestSent struct{}

func (evt CacheRequestSent) apply(state *CarfileCacheInfo) {
	state.CandidateReplicaFailures = make([]string, 0)
	state.EdgeReplicaFailures = make([]string, 0)
}

// Normal path

// CarfileStartCaches start caches
type CarfileStartCaches struct {
	ID                          string
	CarfileHash                 CarfileHash
	Replicas                    int64
	ServerID                    string
	CreatedAt                   int64
	Expiration                  int64
	CandidateReplicaCachesCount int
}

func (evt CarfileStartCaches) apply(state *CarfileCacheInfo) {
	state.CarfileCID = evt.ID
	state.CarfileHash = evt.CarfileHash
	state.EdgeReplicas = evt.Replicas
	state.ServerID = evt.ServerID
	state.CreatedAt = evt.CreatedAt
	state.Expiration = evt.Expiration
	state.CandidateReplicas = int64(seedCacheCount + evt.CandidateReplicaCachesCount)
}

// CarfileReCache get first carfile to candidate
type CarfileReCache struct{}

func (evt CarfileReCache) apply(state *CarfileCacheInfo) {}

// CacheSucceed nodes cache carfile completed
type CacheSucceed struct{}

func (evt CacheSucceed) apply(state *CarfileCacheInfo) {
	state.RetryCount = 0
}

// CacheFailed nodes cache carfile failed
type CacheFailed struct{ error }

// FormatError Format error
func (evt CacheFailed) FormatError(xerrors.Printer) (next error) { return evt.error }

func (evt CacheFailed) apply(state *CarfileCacheInfo) {
	state.RetryCount++
}