package storage

import (
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

// Normal path

type CarfileStart struct {
	ID CarfileID
}

func (evt CarfileStart) apply(state *CarfileInfo) {
	state.CarfileCID = evt.ID
}

type CarfileFinished struct {
	ID CarfileID
}

func (evt CarfileFinished) apply(state *CarfileInfo) {
	state.CarfileCID = evt.ID
}

type PreParing struct {
	ID CarfileID
}

func (evt PreParing) apply(state *CarfileInfo) {
	state.CarfileCID = evt.ID
}
