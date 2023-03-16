package storage

import (
	"github.com/linguohua/titan/api/types"
	"golang.org/x/xerrors"
)

type mutator interface {
	apply(state *CarfileInfo)
}

// globalMutator is an event which can apply in every state
type globalMutator interface {
	// applyGlobal applies the event to the state. If if returns true,
	//  event processing should be interrupted
	applyGlobal(state *CarfileInfo) bool
}

// Ignorable Ignorable
type Ignorable interface {
	Ignore()
}

// Global events

// CarfileRestart restart
type CarfileRestart struct{}

func (evt CarfileRestart) applyGlobal(state *CarfileInfo) bool {
	state.RetryCount = 0
	return false
}

// CarfileFatalError Carfile fatal error
type CarfileFatalError struct{ error }

// FormatError Format error
func (evt CarfileFatalError) FormatError(xerrors.Printer) (next error) { return evt.error }

func (evt CarfileFatalError) applyGlobal(state *CarfileInfo) bool {
	log.Errorf("Fatal error on carfile %s: %+v", state.CarfileCID, evt.error)
	return true
}

// CarfileForceState carfile force
type CarfileForceState struct {
	State CarfileState
}

func (evt CarfileForceState) applyGlobal(state *CarfileInfo) bool {
	state.State = evt.State
	return true
}

// CarfileRemove remove
type CarfileRemove struct{}

func (evt CarfileRemove) applyGlobal(state *CarfileInfo) bool {
	*state = CarfileInfo{State: Removing}
	return true
}

// CarfileInfoUpdate update carfile info
type CarfileInfoUpdate struct {
	ResultInfo *CacheResultInfo
}

func (evt CarfileInfoUpdate) applyGlobal(state *CarfileInfo) bool {
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
	ResultInfo *CacheResultInfo
}

func (evt CacheResult) apply(state *CarfileInfo) {
	rInfo := evt.ResultInfo
	if rInfo == nil {
		return
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

func (evt CacheRequestSent) apply(state *CarfileInfo) {
	state.CandidateReplicaFailures = make([]string, 0)
	state.EdgeReplicaFailures = make([]string, 0)
}

// Normal path

// CarfileStartCaches start caches
type CarfileStartCaches struct {
	ID          string
	CarfileHash CarfileHash
	Replicas    int64
	ServerID    string
	CreatedAt   int64
	Expiration  int64
}

func (evt CarfileStartCaches) apply(state *CarfileInfo) {
	state.CarfileCID = evt.ID
	state.CarfileHash = evt.CarfileHash
	state.EdgeReplicas = evt.Replicas
	state.ServerID = evt.ServerID
	state.CreatedAt = evt.CreatedAt
	state.Expiration = evt.Expiration
	state.CandidateReplicas = int64(rootCachesCount + candidateReplicaCachesCount)
}

// CarfileReCache get first carfile to candidate
type CarfileReCache struct{}

func (evt CarfileReCache) apply(state *CarfileInfo) {}

// CacheSucceed nodes cache carfile completed
type CacheSucceed struct{}

func (evt CacheSucceed) apply(state *CarfileInfo) {
	state.RetryCount = 0
}

// CacheFailed nodes cache carfile failed
type CacheFailed struct{ error }

// FormatError Format error
func (evt CacheFailed) FormatError(xerrors.Printer) (next error) { return evt.error }

func (evt CacheFailed) apply(state *CarfileInfo) {
	state.RetryCount++
}
