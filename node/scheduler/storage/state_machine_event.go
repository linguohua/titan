package storage

import (
	"golang.org/x/xerrors"
	"time"
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

// Normal path

type CarfileStartCache struct {
	ID          CarfileID
	CarfileHash string    `db:"carfile_hash"`
	Replicas    int       `db:"s"`
	NodeID      string    `db:"node_id"`
	ServerID    string    `db:"server_id"`
	CreatedAt   time.Time `db:"created_at"`
	Expiration  time.Time `db:"expiration"`
}

func (evt CarfileStartCache) apply(state *CarfileInfo) {
	state.CarfileCID = evt.ID
	state.CarfileHash = evt.CarfileHash
	state.Replicas = evt.Replicas
	state.NodeID = evt.NodeID
	state.ServerID = evt.ServerID
	state.CreatedAt = evt.CreatedAt
	state.Expiration = evt.Expiration
}

type CarfileGetSeed struct{}

func (evt CarfileGetSeed) apply(state *CarfileInfo) {}

type CarfileGetSeedCompleted struct{}

func (evt CarfileGetSeedCompleted) apply(state *CarfileInfo) {}

type CarfileCandidateCaching struct{}

func (evt CarfileCandidateCaching) apply(state *CarfileInfo) {}

type CarfileCandidateCompleted struct{}

func (evt CarfileCandidateCompleted) apply(state *CarfileInfo) {}

type CarfileEdgeCaching struct{}

func (evt CarfileEdgeCaching) apply(state *CarfileInfo) {}

type CarfileEdgeCompleted struct{}

func (evt CarfileEdgeCompleted) apply(state *CarfileInfo) {}

type CarfileFinalize struct{}

func (evt CarfileFinalize) apply(state *CarfileInfo) {}

type CarfileGetSeedFailed struct{ error }

func (evt CarfileGetSeedFailed) FormatError(xerrors.Printer) (next error) { return evt.error }
func (evt CarfileGetSeedFailed) apply(ci *CarfileInfo)                    {}

type CarfileCandidateCachingFailed struct{ error }

func (evt CarfileCandidateCachingFailed) FormatError(xerrors.Printer) (next error) {
	return evt.error
}
func (evt CarfileCandidateCachingFailed) apply(ci *CarfileInfo) {
	ci.CandidateStoreFails++
}

type CarfileEdgeCachingFailed struct{ error }

func (evt CarfileEdgeCachingFailed) FormatError(xerrors.Printer) (next error) {
	return evt.error
}
func (evt CarfileEdgeCachingFailed) apply(ci *CarfileInfo) {
	ci.EdgeStoreFails++
}
