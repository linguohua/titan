package assets

import (
	"context"
	"reflect"

	"github.com/filecoin-project/go-statemachine"
	"golang.org/x/xerrors"
)

// Plan Plan
func (m *Manager) Plan(events []statemachine.Event, user interface{}) (interface{}, uint64, error) {
	next, processed, err := m.plan(events, user.(*AssetPullingInfo))
	if err != nil || next == nil {
		return nil, processed, nil
	}

	return func(ctx statemachine.Context, si AssetPullingInfo) error {
		err := next(ctx, si)
		if err != nil {
			log.Errorf("unhandled error (%s): %+v", si.CID, err)
			return nil
		}

		return nil
	}, processed, nil
}

var planners = map[AssetState]func(events []statemachine.Event, state *AssetPullingInfo) (uint64, error){
	// external import
	UndefinedState: planOne(
		on(AssetStartPulls{}, FindFirstCandidate),
	),
	FindFirstCandidate: planOne(
		on(PullRequestSent{}, AssetSeedPulling),
		on(PullFailed{}, SeedPullFailed),
	),
	AssetSeedPulling: planOne(
		on(PullSucceed{}, FindCandidatesToPull),
		on(PullFailed{}, SeedPullFailed),
		apply(PulledResult{}),
	),
	FindCandidatesToPull: planOne(
		on(PullRequestSent{}, CandidatesPulling),
		on(PullSucceed{}, FindEdgesToPull),
		on(PullFailed{}, CandidatesPullFailed),
	),
	CandidatesPulling: planOne(
		on(PullFailed{}, CandidatesPullFailed),
		on(PullSucceed{}, FindEdgesToPull),
		apply(PulledResult{}),
	),
	FindEdgesToPull: planOne(
		on(PullRequestSent{}, EdgesPulling),
		on(PullFailed{}, EdgesPullFailed),
		on(PullSucceed{}, Finished),
	),
	EdgesPulling: planOne(
		on(PullFailed{}, EdgesPullFailed),
		on(PullSucceed{}, Finished),
		apply(PulledResult{}),
	),
	SeedPullFailed: planOne(
		on(AssetRePull{}, FindFirstCandidate),
	),
	CandidatesPullFailed: planOne(
		on(AssetRePull{}, FindCandidatesToPull),
	),
	EdgesPullFailed: planOne(
		on(AssetRePull{}, FindEdgesToPull),
	),
	Finished: planOne(
		on(AssetRePull{}, FindCandidatesToPull),
	),
	Remove: planOne(
		on(AssetStartPulls{}, FindFirstCandidate),
	),
}

func (m *Manager) plan(events []statemachine.Event, state *AssetPullingInfo) (func(statemachine.Context, AssetPullingInfo) error, uint64, error) {
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

	log.Debugf("%s: %s", state.Hash, state.State)

	switch state.State {
	// Happy path
	case FindFirstCandidate:
		return m.handleFindFirstCandidate, processed, nil
	case AssetSeedPulling:
		return m.handleAssetSeedPulling, processed, nil
	case FindCandidatesToPull:
		return m.handleFindCandidates, processed, nil
	case FindEdgesToPull:
		return m.handleFindEdges, processed, nil
	case CandidatesPulling:
		return m.handleCandidatesPulling, processed, nil
	case EdgesPulling:
		return m.handleEdgesPulling, processed, nil
	case Finished:
		return m.handleFinished, processed, nil
	case SeedPullFailed, CandidatesPullFailed, EdgesPullFailed:
		return m.handlePullsFailed, processed, nil
	case Remove:
		return nil, processed, nil
	// Fatal errors
	case UndefinedState:
		log.Error("asset update with undefined state!")
	default:
		log.Errorf("unexpected asset update state: %s", state.State)
	}

	return nil, processed, nil
}

func planOne(ts ...func() (mut mutator, next func(info *AssetPullingInfo) (more bool, err error))) func(events []statemachine.Event, state *AssetPullingInfo) (uint64, error) {
	return func(events []statemachine.Event, state *AssetPullingInfo) (uint64, error) {
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
					log.Warnf("asset %s got error event %T: %+v", state.CID, event.User, err)
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

func on(mut mutator, next AssetState) func() (mutator, func(*AssetPullingInfo) (bool, error)) {
	return func() (mutator, func(*AssetPullingInfo) (bool, error)) {
		return mut, func(state *AssetPullingInfo) (bool, error) {
			state.State = next
			return false, nil
		}
	}
}

// like `on`, but doesn't change state
func apply(mut mutator) func() (mutator, func(*AssetPullingInfo) (bool, error)) {
	return func() (mutator, func(*AssetPullingInfo) (bool, error)) {
		return mut, func(state *AssetPullingInfo) (bool, error) {
			return true, nil
		}
	}
}

func (m *Manager) restartStateMachines(ctx context.Context) error {
	defer m.statemachineWait.Done()

	list, err := m.ListAssets()
	if err != nil {
		log.Errorf("loading assets: %+v", err)
		return err
	}

	for _, asset := range list {
		if err := m.assetStateMachines.Send(asset.Hash, PullAssetRestart{}); err != nil {
			log.Errorf("restarting asset %s: %+v", asset.CID, err)
			continue
		}

		m.addOrResetAssetTicker(asset.Hash.String())
	}

	return nil
}

// ListAssets load asset pull infos from statemachine
func (m *Manager) ListAssets() ([]AssetPullingInfo, error) {
	var list []AssetPullingInfo
	if err := m.assetStateMachines.List(&list); err != nil {
		return nil, err
	}
	return list, nil
}
