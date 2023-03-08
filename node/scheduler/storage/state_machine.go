package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/filecoin-project/go-statemachine"
	"golang.org/x/xerrors"
	"reflect"
	"time"
)

func (m *Manager) Plan(events []statemachine.Event, user interface{}) (interface{}, uint64, error) {
	log.Infof("state machine plan")
	next, processed, err := m.plan(events, user.(*CarfileInfo))
	if err != nil || next == nil {
		l := Log{
			Timestamp: uint64(time.Now().Unix()),
			Message:   fmt.Sprintf("state machine error: %s", err),
			Kind:      fmt.Sprintf("error;%T", err),
		}
		user.(*CarfileInfo).logAppend(l)
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

var fsmPlanners = map[CarfileState]func(events []statemachine.Event, state *CarfileInfo) (uint64, error){
	// external import
	UndefinedCarfileState: planOne(
		on(CarfileStartCache{}, StartCache),
	),
	StartCache: planOne(
		on(CarfileGetSeed{}, GetSeed),
	),
	GetSeed: planOne(
		on(CarfileGetSeedCompleted{}, GetSeedCompleted),
		on(CarfileGetSeedFailed{}, GetSeedFailed),
	),
	GetSeedCompleted: planOne(
		on(CarfileCandidateCaching{}, CandidateCaching),
	),
	CandidateCaching: planOne(
		on(CarfileCandidateCachingFailed{}, CandidateCachingFailed),
		on(CarfileCandidateCompleted{}, CandidateCompleted),
	),
	CandidateCompleted: planOne(
		on(CarfileEdgeCaching{}, EdgeCaching),
	),
	EdgeCaching: planOne(
		on(CarfileEdgeCachingFailed{}, EdgeCachingFailed),
		on(CarfileEdgeCompleted{}, EdgeCompleted),
	),
	EdgeCompleted: planOne(
		on(CarfileFinalize{}, Finalize),
	),
	GetSeedFailed: planOne(
		on(CarfileGetSeed{}, GetSeed),
	),
	CandidateCachingFailed: planOne(
		on(CarfileCandidateCaching{}, CandidateCaching),
	),
	EdgeCachingFailed: planOne(
		on(CarfileEdgeCaching{}, EdgeCaching),
	),
}

func (state *CarfileInfo) logAppend(l Log) {
	if len(state.Log) > 8000 {
		log.Warnw("truncating sector log", "carfile", state.CarfileCID)
		state.Log[2000] = Log{
			Timestamp: uint64(time.Now().Unix()),
			Message:   "truncating log (above 8000 entries)",
			Kind:      fmt.Sprintf("truncate"),
		}

		state.Log = append(state.Log[:2000], state.Log[6000:]...)
	}

	state.Log = append(state.Log, l)
}

func (m *Manager) logEvents(events []statemachine.Event, state *CarfileInfo) {
	for _, event := range events {
		log.Debugw("carfile event", "carfile", state.CarfileCID, "type", fmt.Sprintf("%T", event.User), "event", event.User)

		e, err := json.Marshal(event)
		if err != nil {
			log.Errorf("marshaling event for logging: %+v", err)
			continue
		}

		if event.User == (CarfileRestart{}) {
			continue // don't log on every state machine restart
		}

		if len(e) > 8000 {
			e = []byte(string(e[:8000]) + "... truncated")
		}

		l := Log{
			Timestamp: uint64(time.Now().Unix()),
			Message:   string(e),
			Kind:      fmt.Sprintf("event;%T", event.User),
		}

		if err, iserr := event.User.(xerrors.Formatter); iserr {
			l.Trace = fmt.Sprintf("%+v", err)
		}

		state.logAppend(l)
	}
}

func (m *Manager) plan(events []statemachine.Event, state *CarfileInfo) (func(statemachine.Context, CarfileInfo) error, uint64, error) {
	m.logEvents(events, state)

	p := fsmPlanners[state.State]
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

	//if err := m.onUpdateSector(context.TODO(), state); err != nil {
	//	log.Errorw("update sector stats", "error", err)
	//}

	switch state.State {
	// Happy path
	case StartCache:
		return m.handleStartCache, processed, nil
	case GetSeed:
		return m.handleGetSeed, processed, nil
	case CandidateCaching:
		return m.handleCandidateCaching, processed, nil
	case EdgeCaching:
		return m.handleEdgeCaching, processed, nil
	case Finalize:
		return m.handleFinalize, processed, nil
	case GetSeedFailed:
		return m.handleGetSeedFailed, processed, nil
	case CandidateCachingFailed:
		return m.handleCandidateCachingFailed, processed, nil
	case EdgeCachingFailed:
		return m.handlerEdgeCachingFailed, processed, nil
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

				if err, iserr := event.User.(error); iserr {
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

func (m *Manager) restartCarfiles(ctx context.Context) error {
	log.Infof("restart carfiles")
	defer log.Info("restart carfiles done")
	defer m.startupWait.Done()

	trackedCarfiles, err := m.ListCarfiles()
	if err != nil {
		log.Errorf("loading carfiles list: %+v", err)
	}

	for _, carfile := range trackedCarfiles {
		if err := m.carfiles.Send(carfile.CarfileCID, CarfileRestart{}); err != nil {
			log.Errorf("restarting carfile %s: %+v", carfile.CarfileCID, err)
		}
	}

	return nil
}
