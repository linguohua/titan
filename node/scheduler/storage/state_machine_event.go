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

type Ignorable interface {
	Ignore()
}

// Global events

type CarfileRestart struct{}

func (evt CarfileRestart) applyGlobal(*CarfileInfo) bool { return false }

type CarfileFatalError struct{ error }

func (evt CarfileFatalError) FormatError(xerrors.Printer) (next error) { return evt.error }

func (evt CarfileFatalError) applyGlobal(state *CarfileInfo) bool {
	log.Errorf("Fatal error on carfile %s: %+v", state.CarfileCID, evt.error)
	return true
}

type CarfileForceState struct {
	State CarfileState
}

func (evt CarfileForceState) applyGlobal(state *CarfileInfo) bool {
	state.State = evt.State
	return true
}

type CacheResult struct {
	ResultInfo *CacheResultInfo
}

func (evt CacheResult) apply(state *CarfileInfo) {
	log.Infof("evt:%v", evt.ResultInfo)
	if evt.ResultInfo == nil {
		return
	}

	if evt.ResultInfo.Status == int64(types.CacheStatusSucceeded) {
		if evt.ResultInfo.IsCandidate {
			state.SuccessedCandidateReplicas++
		} else {
			state.SuccessedEdgeReplicas++
		}
		state.Size = evt.ResultInfo.CarfileSize
		state.Blocks = evt.ResultInfo.CarfileBlockCount
	} else {
		if evt.ResultInfo.IsCandidate {
			state.FailedCandidateReplicas++
		} else {
			state.FailedEdgeReplicas++
		}
	}
}

type RequestNodes struct{}

func (evt RequestNodes) apply(state *CarfileInfo) {
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
	state.CandidateReplicas = int64(rootCacheCount + candidateReplicaCacheCount)
}

// CarfileGetSeed get frist carfile to candidate
type CarfileGetSeed struct{}

func (evt CarfileGetSeed) apply(state *CarfileInfo) {}

// CacheSuccessed nodes cache carfile completed
type CacheSuccessed struct{}

func (evt CacheSuccessed) apply(state *CarfileInfo) {
}

type CarfileCandidateCaching struct{}

func (evt CarfileCandidateCaching) apply(state *CarfileInfo) {}

type CarfileEdgeCaching struct{}

func (evt CarfileEdgeCaching) apply(state *CarfileInfo) {}

type CarfileFinalize struct{}

func (evt CarfileFinalize) apply(state *CarfileInfo) {}

type CacheFailed struct{ error }

func (evt CacheFailed) FormatError(xerrors.Printer) (next error) { return evt.error }
func (evt CacheFailed) apply(ci *CarfileInfo) {
	ci.retries++
}
