package storage

import (
	"context"
	"reflect"
	"time"

	"github.com/filecoin-project/go-statemachine"
	"golang.org/x/xerrors"
)

// Plan Plan
func (m *Manager) Plan(events []statemachine.Event, user interface{}) (interface{}, uint64, error) {
	next, processed, err := m.plan(events, user.(*CarfileInfo))
	if err != nil || next == nil {
		return nil, processed, nil
	}

	return func(ctx statemachine.Context, si CarfileInfo) error {
		err := next(ctx, si)
		if err != nil {
			log.Errorf("unhandled carfile error (%s): %+v", si.CarfileCID, err)
			return nil
		}

		return nil
	}, processed, nil // TODO: This processed event count is not very correct
}

var planners = map[CarfileState]func(events []statemachine.Event, state *CarfileInfo) (uint64, error){
	// external import
	UndefinedCarfileState: planOne(
		on(CarfileStartCaches{}, CacheCarfileSeed),
	),
	CacheCarfileSeed: planOne(
		on(CacheRequestSent{}, CarfileSeedCaching),
		on(CacheFailed{}, CacheSeedFailed),
	),
	CarfileSeedCaching: planOne(
		on(CacheSucceed{}, CacheToCandidates),
		on(CacheFailed{}, CacheSeedFailed),
		apply(CacheResult{}),
	),
	CacheToCandidates: planOne(
		on(CacheRequestSent{}, CandidatesCaching),
		on(CacheSucceed{}, CacheToEdges),
		on(CacheFailed{}, CacheCandidatesFailed),
	),
	CandidatesCaching: planOne(
		on(CacheFailed{}, CacheCandidatesFailed),
		on(CacheSucceed{}, CacheToEdges),
		apply(CacheResult{}),
	),
	CacheToEdges: planOne(
		on(CacheRequestSent{}, EdgesCaching),
		on(CacheFailed{}, CacheEdgesFailed),
		on(CacheSucceed{}, Finalize),
	),
	EdgesCaching: planOne(
		on(CacheFailed{}, CacheEdgesFailed),
		on(CacheSucceed{}, Finalize),
		apply(CacheResult{}),
	),
	CacheSeedFailed: planOne(
		on(CarfileReCache{}, CacheCarfileSeed),
	),
	CacheCandidatesFailed: planOne(
		on(CarfileReCache{}, CacheToCandidates),
	),
	CacheEdgesFailed: planOne(
		on(CarfileReCache{}, CacheToEdges),
	),
	Finalize: planOne(
		on(CarfileReCache{}, CacheToCandidates),
	),
	Removing: planOne(
		on(CarfileStartCaches{}, CacheCarfileSeed),
	),
}

func (m *Manager) plan(events []statemachine.Event, state *CarfileInfo) (func(statemachine.Context, CarfileInfo) error, uint64, error) {
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
		return m.handleCacheSeed, processed, nil
	case CarfileSeedCaching:
		return m.handleSeedCaching, processed, nil
	case CacheToCandidates:
		return m.handleCacheToCandidates, processed, nil
	case CacheToEdges:
		return m.handleCacheToEdges, processed, nil
	case CandidatesCaching:
		return m.handleCandidatesCaching, processed, nil
	case EdgesCaching:
		return m.handleEdgesCaching, processed, nil
	case Finalize:
		return m.handleFinalize, processed, nil
	case CacheSeedFailed, CacheCandidatesFailed, CacheEdgesFailed:
		return m.handleCachesFailed, processed, nil
	case Removing:
		return nil, processed, nil
	// Fatal errors
	case UndefinedCarfileState:
		log.Error("carfile update with undefined state!")
	default:
		log.Errorf("unexpected carfile update state: %s", state.State)
	}

	return nil, processed, nil
}

func planOne(ts ...func() (mut mutator, next func(info *CarfileInfo) (more bool, err error))) func(events []statemachine.Event, state *CarfileInfo) (uint64, error) {
	return func(events []statemachine.Event, state *CarfileInfo) (uint64, error) {
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

// planOne but ignores unhandled states without error, this prevents the need to handle all possible events creating
// error during forced override
func planOneOrIgnore(ts ...func() (mut mutator, next func(info *CarfileInfo) (more bool, err error))) func(events []statemachine.Event, state *CarfileInfo) (uint64, error) {
	f := planOne(ts...)
	return func(events []statemachine.Event, state *CarfileInfo) (uint64, error) {
		cnt, err := f(events, state)
		if err != nil {
			log.Warnf("planOneOrIgnore: ignoring error from planOne: %s", err)
		}
		return cnt, nil
	}
}

func on(mut mutator, next CarfileState) func() (mutator, func(*CarfileInfo) (bool, error)) {
	return func() (mutator, func(*CarfileInfo) (bool, error)) {
		return mut, func(state *CarfileInfo) (bool, error) {
			state.State = next
			return false, nil
		}
	}
}

// like `on`, but doesn't change state
func apply(mut mutator) func() (mutator, func(*CarfileInfo) (bool, error)) {
	return func() (mutator, func(*CarfileInfo) (bool, error)) {
		return mut, func(state *CarfileInfo) (bool, error) {
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

	// wait nodes connect
	time.Sleep(1 * time.Minute)

	for _, carfile := range trackedCarfiles {
		if err := m.carfiles.Send(carfile.CarfileHash, CarfileRestart{}); err != nil {
			log.Errorf("restarting carfile %s: %+v", carfile.CarfileCID, err)
			continue
		}

		m.addOrResetCarfileTicker(carfile.CarfileHash.String())
	}

	return nil
}

// ListCarfiles load carfiles from db
func (m *Manager) ListCarfiles() ([]CarfileInfo, error) {
	var carfiles []CarfileInfo
	if err := m.carfiles.List(&carfiles); err != nil {
		return nil, err
	}
	return carfiles, nil
}
