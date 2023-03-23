package caching

import (
	"time"

	"github.com/filecoin-project/go-statemachine"
	"golang.org/x/xerrors"
)

var (
	// MinRetryTime retry time
	MinRetryTime = 1 * time.Minute

	// MaxRetryCount retry count
	MaxRetryCount = 3
)

func failedCoolDown(ctx statemachine.Context, info CarfileCacheInfo) error {
	// TODO: Exponential back off when we see consecutive failures

	retryStart := time.Now().Add(MinRetryTime)
	if time.Now().Before(retryStart) {
		log.Debugf("%s(%s), waiting %s before retrying", info.State, info.CarfileHash, time.Until(retryStart))
		select {
		case <-time.After(time.Until(retryStart)):
		case <-ctx.Context().Done():
			return ctx.Context().Err()
		}
	}

	return nil
}

func (m *Manager) handleCacheCarfileSeed(ctx statemachine.Context, info CarfileCacheInfo) error {
	log.Debugf("handle cache seed: %s", info.CarfileCID)

	// find nodes
	nodes := m.findCandidates(seedCacheCount, info.CandidateReplicaSucceeds)
	if len(nodes) < 1 {
		return ctx.Send(CacheFailed{error: xerrors.New("node not found")})
	}

	// save to db
	err := m.saveCandidateReplicaInfos(nodes, info.CarfileHash.String())
	if err != nil {
		return ctx.Send(CacheFailed{error: err})
	}

	m.addOrResetCarfileTicker(info.CarfileHash.String())

	// send a cache request to the node
	go func() {
		for _, node := range nodes {
			err := node.API().CacheCarfile(ctx.Context(), info.CarfileCID, nil)
			if err != nil {
				log.Errorf("%s CacheCarfile err:%s", node.NodeID, err.Error())
				continue
			}

			node.IncrCurCacheCount(1)
		}
	}()

	return ctx.Send(CacheRequestSent{})
}

func (m *Manager) handleCarfileSeedCaching(ctx statemachine.Context, info CarfileCacheInfo) error {
	log.Debugf("handle seed caching, %s", info.CarfileCID)

	if len(info.CandidateReplicaSucceeds) >= seedCacheCount {
		return ctx.Send(CacheSucceed{})
	}

	if len(info.CandidateReplicaSucceeds)+len(info.CandidateReplicaFailures) >= seedCacheCount {
		return ctx.Send(CacheFailed{error: xerrors.New("node cache failed")})
	}

	return nil
}

func (m *Manager) handleFindCandidatesToCache(ctx statemachine.Context, info CarfileCacheInfo) error {
	log.Debugf("handle cache to candidates, %s", info.CarfileCID)

	needCount := info.CandidateReplicas - int64(len(info.CandidateReplicaSucceeds))
	if needCount < 1 {
		// cache to edges
		return ctx.Send(CacheSucceed{})
	}

	// find nodes
	nodes := m.findCandidates(int(needCount), info.CandidateReplicaSucceeds)
	if len(nodes) < 1 {
		return ctx.Send(CacheFailed{error: xerrors.New("node not found")})
	}

	// save to db
	err := m.saveCandidateReplicaInfos(nodes, info.CarfileHash.String())
	if err != nil {
		return ctx.Send(CacheFailed{error: err})
	}

	sources := m.Sources(info.CarfileHash.String(), info.CandidateReplicaSucceeds)

	m.addOrResetCarfileTicker(info.CarfileHash.String())

	// send a cache request to the node
	go func() {
		for _, node := range nodes {
			err := node.API().CacheCarfile(ctx.Context(), info.CarfileCID, sources)
			if err != nil {
				log.Errorf("%s CacheCarfile err:%s", node.NodeID, err.Error())
				continue
			}

			node.IncrCurCacheCount(1)
		}
	}()

	return ctx.Send(CacheRequestSent{})
}

func (m *Manager) handleCandidatesCaching(ctx statemachine.Context, info CarfileCacheInfo) error {
	log.Debugf("handle candidates caching, %s", info.CarfileCID)

	if int64(len(info.CandidateReplicaSucceeds)) >= info.CandidateReplicas {
		return ctx.Send(CacheSucceed{})
	}

	if int64(len(info.CandidateReplicaSucceeds)+len(info.CandidateReplicaFailures)) >= info.CandidateReplicas {
		return ctx.Send(CacheFailed{error: xerrors.New("node cache failed")})
	}

	return nil
}

func (m *Manager) handleFindEdgesToCache(ctx statemachine.Context, info CarfileCacheInfo) error {
	log.Debugf("handle cache to edges , %s", info.CarfileCID)

	needCount := info.EdgeReplicas - int64(len(info.EdgeReplicaSucceeds))
	if needCount < 1 {
		return ctx.Send(CacheSucceed{})
	}

	sources := m.Sources(info.CarfileHash.String(), info.CandidateReplicaSucceeds)
	if len(sources) < 1 {
		return ctx.Send(CacheFailed{error: xerrors.New("source node not found")})
	}

	// find nodes
	nodes := m.findEdges(int(needCount), info.EdgeReplicaSucceeds)
	if len(nodes) < 1 {
		return ctx.Send(CacheFailed{error: xerrors.New("node not found")})
	}

	// save to db
	err := m.saveEdgeReplicaInfos(nodes, info.CarfileHash.String())
	if err != nil {
		return ctx.Send(CacheFailed{error: err})
	}

	m.addOrResetCarfileTicker(info.CarfileHash.String())

	// send a cache request to the node
	go func() {
		for _, node := range nodes {
			err := node.API().CacheCarfile(ctx.Context(), info.CarfileCID, sources)
			if err != nil {
				log.Errorf("%s CacheCarfile err:%s", node.NodeID, err.Error())
				continue
			}

			node.IncrCurCacheCount(1)
		}
	}()

	return ctx.Send(CacheRequestSent{})
}

func (m *Manager) handleEdgesCaching(ctx statemachine.Context, info CarfileCacheInfo) error {
	log.Debugf("handle edge caching, %s", info.CarfileCID)
	if int64(len(info.EdgeReplicaSucceeds)) >= info.EdgeReplicas {
		return ctx.Send(CacheSucceed{})
	}

	if int64(len(info.EdgeReplicaSucceeds)+len(info.EdgeReplicaFailures)) >= info.EdgeReplicas {
		return ctx.Send(CacheFailed{error: xerrors.New("node cache failed")})
	}

	return nil
}

func (m *Manager) handleFinished(ctx statemachine.Context, info CarfileCacheInfo) error {
	log.Debugf("handle carfile finalize: %s", info.CarfileCID)
	m.removeCarfileTicker(info.CarfileHash.String())

	return nil
}

func (m *Manager) handleCachesFailed(ctx statemachine.Context, info CarfileCacheInfo) error {
	m.removeCarfileTicker(info.CarfileHash.String())

	if info.RetryCount >= int64(MaxRetryCount) {
		log.Debugf("handle caches failed: %s reached the max retry count(%d), stop retrying", info.CarfileHash, info.RetryCount)
		return nil
	}

	log.Debugf("handle caches failed: %s, retries: %d", info.CarfileHash, info.RetryCount+1)

	if err := failedCoolDown(ctx, info); err != nil {
		return err
	}

	return ctx.Send(CarfileReCache{})
}
