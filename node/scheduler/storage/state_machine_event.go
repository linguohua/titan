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

func (evt CarfileRestart) applyGlobal(*CarfileInfo) bool { return false }

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
	state.State = Remove
	return true
}

// CacheResult nodes cache result
type CacheResult struct {
	ResultInfo *CacheResultInfo
}

func (evt CacheResult) apply(state *CarfileInfo) {
	if evt.ResultInfo == nil {
		return
	}

	if evt.ResultInfo.Status == int64(types.CacheStatusSucceeded) {
		if evt.ResultInfo.IsCandidate {
			state.CandidateReplicaSuccesses++
		} else {
			state.EdgeReplicaSuccesses++
		}
		state.Size = evt.ResultInfo.CarfileSize
		state.Blocks = evt.ResultInfo.CarfileBlockCount
	} else {
		if evt.ResultInfo.IsCandidate {
			state.CandidateReplicaFailures++
		} else {
			state.EdgeReplicaFailures++
		}
	}
}

// CacheRequestSent request nodes cache carfile
type CacheRequestSent struct{}

func (evt CacheRequestSent) apply(state *CarfileInfo) {
	state.CandidateReplicaFailures = 0
	state.EdgeReplicaFailures = 0
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

// CarfileRecache get frist carfile to candidate
type CarfileRecache struct{}

func (evt CarfileRecache) apply(state *CarfileInfo) {}

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
