package assets

import (
	"context"
	"reflect"

	"github.com/filecoin-project/go-statemachine"
	"golang.org/x/xerrors"
)

// Plan Plan
func (m *Manager) Plan(events []statemachine.Event, user interface{}) (interface{}, uint64, error) {
	next, processed, err := m.plan(events, user.(*AssetCachingInfo))
	if err != nil || next == nil {
		return nil, processed, nil
	}

	return func(ctx statemachine.Context, si AssetCachingInfo) error {
		err := next(ctx, si)
		if err != nil {
			log.Errorf("unhandled error (%s): %+v", si.CID, err)
			return nil
		}

		return nil
	}, processed, nil
}

var planners = map[AssetState]func(events []statemachine.Event, state *AssetCachingInfo) (uint64, error){
	// external import
	UndefinedState: planOne(
		on(AssetStartCaches{}, CacheAssetSeed),
	),
	CacheAssetSeed: planOne(
		on(CacheRequestSent{}, AssetSeedCaching),
		on(CacheFailed{}, SeedCacheFailed),
	),
	AssetSeedCaching: planOne(
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
		on(AssetReCache{}, CacheAssetSeed),
	),
	CandidatesCacheFailed: planOne(
		on(AssetReCache{}, FindCandidatesToCache),
	),
	EdgesCacheFailed: planOne(
		on(AssetReCache{}, FindEdgesToCache),
	),
	Finished: planOne(
		on(AssetReCache{}, FindCandidatesToCache),
	),
	Remove: planOne(
		on(AssetStartCaches{}, CacheAssetSeed),
	),
}

func (m *Manager) plan(events []statemachine.Event, state *AssetCachingInfo) (func(statemachine.Context, AssetCachingInfo) error, uint64, error) {
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
	case CacheAssetSeed:
		return m.handleCacheAssetSeed, processed, nil
	case AssetSeedCaching:
		return m.handleAssetSeedCaching, processed, nil
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

func planOne(ts ...func() (mut mutator, next func(info *AssetCachingInfo) (more bool, err error))) func(events []statemachine.Event, state *AssetCachingInfo) (uint64, error) {
	return func(events []statemachine.Event, state *AssetCachingInfo) (uint64, error) {
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

func on(mut mutator, next AssetState) func() (mutator, func(*AssetCachingInfo) (bool, error)) {
	return func() (mutator, func(*AssetCachingInfo) (bool, error)) {
		return mut, func(state *AssetCachingInfo) (bool, error) {
			state.State = next
			return false, nil
		}
	}
}

// like `on`, but doesn't change state
func apply(mut mutator) func() (mutator, func(*AssetCachingInfo) (bool, error)) {
	return func() (mutator, func(*AssetCachingInfo) (bool, error)) {
		return mut, func(state *AssetCachingInfo) (bool, error) {
			return true, nil
		}
	}
}

func (m *Manager) restartStateMachines(ctx context.Context) error {
	defer m.startupWait.Done()

	list, err := m.ListAssets()
	if err != nil {
		log.Errorf("loading assets: %+v", err)
		return err
	}

	for _, asset := range list {
		if err := m.assetStateMachines.Send(asset.Hash, CacheAssetRestart{}); err != nil {
			log.Errorf("restarting asset %s: %+v", asset.CID, err)
			continue
		}

		m.addOrResetAssetTicker(asset.Hash.String())
	}

	return nil
}

// ListAssets load asset cache infos from statemachine
func (m *Manager) ListAssets() ([]AssetCachingInfo, error) {
	var list []AssetCachingInfo
	if err := m.assetStateMachines.List(&list); err != nil {
		return nil, err
	}
	return list, nil
}
