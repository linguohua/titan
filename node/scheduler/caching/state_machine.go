package caching

import (
	"context"
	"reflect"

	"github.com/filecoin-project/go-statemachine"
	"golang.org/x/xerrors"
)

// Plan Plan
func (m *Manager) Plan(events []statemachine.Event, user interface{}) (interface{}, uint64, error) {
	next, processed, err := m.plan(events, user.(*CarfileCacheInfo))
	if err != nil || next == nil {
		return nil, processed, nil
	}

	return func(ctx statemachine.Context, si CarfileCacheInfo) error {
		err := next(ctx, si)
		if err != nil {
			log.Errorf("unhandled carfile error (%s): %+v", si.CarfileCID, err)
			return nil
		}

		return nil
	}, processed, nil
}

var planners = map[CarfileState]func(events []statemachine.Event, state *CarfileCacheInfo) (uint64, error){
	// external import
	UndefinedState: planOne(
		on(CarfileStartCaches{}, CacheCarfileSeed),
	),
	CacheCarfileSeed: planOne(
		on(CacheRequestSent{}, CarfileSeedCaching),
		on(CacheFailed{}, SeedCacheFailed),
	),
	CarfileSeedCaching: planOne(
		on(CacheSucceed{}, FindCandidatesToCache),
		on(CacheFailed{}, SeedCacheFailed),
		apply(CacheResult{}),
	),
	FindCandidatesToCache: planOne(
		on(CacheRequestSent{}, CandidatesCaching),
		on(CacheSucceed{}, FindEdgesToCache),
		on(CacheFailed{}, CandidatesCacheFailed),
	),
	CandidatesCaching: planOne(
		on(CacheFailed{}, CandidatesCacheFailed),
		on(CacheSucceed{}, FindEdgesToCache),
		apply(CacheResult{}),
	),
	FindEdgesToCache: planOne(
		on(CacheRequestSent{}, EdgesCaching),
		on(CacheFailed{}, EdgesCacheFailed),
		on(CacheSucceed{}, Finished),
	),
	EdgesCaching: planOne(
		on(CacheFailed{}, EdgesCacheFailed),
		on(CacheSucceed{}, Finished),
		apply(CacheResult{}),
	),
	SeedCacheFailed: planOne(
		on(CarfileReCache{}, CacheCarfileSeed),
	),
	CandidatesCacheFailed: planOne(
		on(CarfileReCache{}, FindCandidatesToCache),
	),
	EdgesCacheFailed: planOne(
		on(CarfileReCache{}, FindEdgesToCache),
	),
	Finished: planOne(
		on(CarfileReCache{}, FindCandidatesToCache),
	),
	Removing: planOne(
		on(CarfileStartCaches{}, CacheCarfileSeed),
	),
}

func (m *Manager) plan(events []statemachine.Event, state *CarfileCacheInfo) (func(statemachine.Context, CarfileCacheInfo) error, uint64, error) {
	p := planners[state.State]
	if p == nil {
		if len(events) == 1 {
			if _, ok := events[0].User.(globalMutator); ok {
				p = planOne() // in case we're in a really weird state, allow restart / update state / remove
			}
		}

		if p == nil {
			return nil, 0, xerrors.Errorf("planner for state %s not found", state.State)
		}
	}

	processed, err := p(events, state)
	if err != nil {
		return nil, processed, xerrors.Errorf("running planner for state %s failed: %w", state.State, err)
	}

	log.Debugf("%s: %s", state.CarfileHash, state.State)

	switch state.State {
	// Happy path
	case CacheCarfileSeed:
		return m.handleCacheCarfileSeed, processed, nil
	case CarfileSeedCaching:
		return m.handleCarfileSeedCaching, processed, nil
	case FindCandidatesToCache:
		return m.handleFindCandidatesToCache, processed, nil
	case FindEdgesToCache:
		return m.handleFindEdgesToCache, processed, nil
	case CandidatesCaching:
		return m.handleCandidatesCaching, processed, nil
	case EdgesCaching:
		return m.handleEdgesCaching, processed, nil
	case Finished:
		return m.handleFinished, processed, nil
	case SeedCacheFailed, CandidatesCacheFailed, EdgesCacheFailed:
		return m.handleCachesFailed, processed, nil
	case Removing:
		return nil, processed, nil
	// Fatal errors
	case UndefinedState:
		log.Error("carfile update with undefined state!")
	default:
		log.Errorf("unexpected carfile update state: %s", state.State)
	}

	return nil, processed, nil
}

func planOne(ts ...func() (mut mutator, next func(info *CarfileCacheInfo) (more bool, err error))) func(events []statemachine.Event, state *CarfileCacheInfo) (uint64, error) {
	return func(events []statemachine.Event, state *CarfileCacheInfo) (uint64, error) {
	eloop:
		for i, event := range events {
			if gm, ok := event.User.(globalMutator); ok {
				gm.applyGlobal(state)
				return uint64(i + 1), nil
			}

			for _, t := range ts {
				mut, next := t()

				if reflect.TypeOf(event.User) != reflect.TypeOf(mut) {
					continue
				}

				if err, isErr := event.User.(error); isErr {
					log.Warnf("carfile %s got error event %T: %+v", state.CarfileCID, event.User, err)
				}

				event.User.(mutator).apply(state)
				more, err := next(state)
				if err != nil || !more {
					return uint64(i + 1), err
				}

				continue eloop
			}

			_, ok := event.User.(Ignorable)
			if ok {
				continue
			}

			return uint64(i + 1), xerrors.Errorf("planner for state %s received unexpected event %T (%+v)", state.State, event.User, event)
		}

		return uint64(len(events)), nil
	}
}

func on(mut mutator, next CarfileState) func() (mutator, func(*CarfileCacheInfo) (bool, error)) {
	return func() (mutator, func(*CarfileCacheInfo) (bool, error)) {
		return mut, func(state *CarfileCacheInfo) (bool, error) {
			state.State = next
			return false, nil
		}
	}
}

// like `on`, but doesn't change state
func apply(mut mutator) func() (mutator, func(*CarfileCacheInfo) (bool, error)) {
	return func() (mutator, func(*CarfileCacheInfo) (bool, error)) {
		return mut, func(state *CarfileCacheInfo) (bool, error) {
			return true, nil
		}
	}
}

func (m *Manager) restartCarfiles(ctx context.Context) error {
	defer m.startupWait.Done()

	trackedCarfiles, err := m.ListCarfiles()
	if err != nil {
		log.Errorf("loading carfiles list: %+v", err)
		return err
	}

	for _, carfile := range trackedCarfiles {
		if err := m.carfiles.Send(carfile.CarfileHash, CarfileRestart{}); err != nil {
			log.Errorf("restarting carfile %s: %+v", carfile.CarfileCID, err)
			continue
		}

		m.addOrResetCarfileTicker(carfile.CarfileHash.String())
	}

	return nil
}

// ListCarfiles load carfile cache infos from statemachine
func (m *Manager) ListCarfiles() ([]CarfileCacheInfo, error) {
	var carfiles []CarfileCacheInfo
	if err := m.carfiles.List(&carfiles); err != nil {
		return nil, err
	}
	return carfiles, nil
}
