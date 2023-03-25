package assets

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

func failedCoolDown(ctx statemachine.Context, info AssetCachingInfo) error {
	retryStart := time.Now().Add(MinRetryTime)
	if time.Now().Before(retryStart) {
		log.Debugf("%s(%s), waiting %s before retrying", info.State, info.Hash, time.Until(retryStart))
		select {
		case <-time.After(time.Until(retryStart)):
		case <-ctx.Context().Done():
			return ctx.Context().Err()
		}
	}

	return nil
}

func (m *Manager) handleCacheAssetSeed(ctx statemachine.Context, info AssetCachingInfo) error {
	log.Debugf("handle cache seed: %s", info.CID)

	// find nodes
	nodes := m.findCandidates(seedCacheCount, info.CandidateReplicaSucceeds)
	if len(nodes) < 1 {
		return ctx.Send(CacheFailed{error: xerrors.New("node not found")})
	}

	// save to db
	err := m.saveCandidateReplicaInfos(nodes, info.Hash.String())
	if err != nil {
		return ctx.Send(CacheFailed{error: err})
	}

	m.addOrResetAssetTicker(info.Hash.String())

	// send a cache request to the node
	go func() {
		for _, node := range nodes {
			err := node.API().CacheCarfile(ctx.Context(), info.CID, nil)
			if err != nil {
				log.Errorf("%s Cache asset err:%s", node.NodeID, err.Error())
				continue
			}

			node.IncrCurCacheCount(1)
		}
	}()

	return ctx.Send(CacheRequestSent{})
}

func (m *Manager) handleAssetSeedCaching(ctx statemachine.Context, info AssetCachingInfo) error {
	log.Debugf("handle seed caching, %s", info.CID)

	if len(info.CandidateReplicaSucceeds) >= seedCacheCount {
		return ctx.Send(CacheSucceed{})
	}

	if len(info.CandidateReplicaSucceeds)+len(info.CandidateReplicaFailures) >= seedCacheCount {
		return ctx.Send(CacheFailed{error: xerrors.New("node cache failed")})
	}

	return nil
}

func (m *Manager) handleFindCandidatesToCache(ctx statemachine.Context, info AssetCachingInfo) error {
	log.Debugf("handle cache to candidates, %s", info.CID)

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
	err := m.saveCandidateReplicaInfos(nodes, info.Hash.String())
	if err != nil {
		return ctx.Send(CacheFailed{error: err})
	}

	sources := m.Sources(info.Hash.String(), info.CandidateReplicaSucceeds)

	m.addOrResetAssetTicker(info.Hash.String())

	// send a cache request to the node
	go func() {
		for _, node := range nodes {
			err := node.API().CacheCarfile(ctx.Context(), info.CID, sources)
			if err != nil {
				log.Errorf("%s Cache asset err:%s", node.NodeID, err.Error())
				continue
			}

			node.IncrCurCacheCount(1)
		}
	}()

	return ctx.Send(CacheRequestSent{})
}

func (m *Manager) handleCandidatesCaching(ctx statemachine.Context, info AssetCachingInfo) error {
	log.Debugf("handle candidates caching, %s", info.CID)

	if int64(len(info.CandidateReplicaSucceeds)) >= info.CandidateReplicas {
		return ctx.Send(CacheSucceed{})
	}

	if int64(len(info.CandidateReplicaSucceeds)+len(info.CandidateReplicaFailures)) >= info.CandidateReplicas {
		return ctx.Send(CacheFailed{error: xerrors.New("node cache failed")})
	}

	return nil
}

func (m *Manager) handleFindEdgesToCache(ctx statemachine.Context, info AssetCachingInfo) error {
	log.Debugf("handle cache to edges , %s", info.CID)

	needCount := info.EdgeReplicas - int64(len(info.EdgeReplicaSucceeds))
	if needCount < 1 {
		return ctx.Send(CacheSucceed{})
	}

	sources := m.Sources(info.Hash.String(), info.CandidateReplicaSucceeds)
	if len(sources) < 1 {
		return ctx.Send(CacheFailed{error: xerrors.New("source node not found")})
	}

	// find nodes
	nodes := m.findEdges(int(needCount), info.EdgeReplicaSucceeds)
	if len(nodes) < 1 {
		return ctx.Send(CacheFailed{error: xerrors.New("node not found")})
	}

	// save to db
	err := m.saveEdgeReplicaInfos(nodes, info.Hash.String())
	if err != nil {
		return ctx.Send(CacheFailed{error: err})
	}

	m.addOrResetAssetTicker(info.Hash.String())

	// send a cache request to the node
	go func() {
		for _, node := range nodes {
			err := node.API().CacheCarfile(ctx.Context(), info.CID, sources)
			if err != nil {
				log.Errorf("%s Cache asset err:%s", node.NodeID, err.Error())
				continue
			}

			node.IncrCurCacheCount(1)
		}
	}()

	return ctx.Send(CacheRequestSent{})
}

func (m *Manager) handleEdgesCaching(ctx statemachine.Context, info AssetCachingInfo) error {
	log.Debugf("handle edge caching, %s", info.CID)
	if int64(len(info.EdgeReplicaSucceeds)) >= info.EdgeReplicas {
		return ctx.Send(CacheSucceed{})
	}

	if int64(len(info.EdgeReplicaSucceeds)+len(info.EdgeReplicaFailures)) >= info.EdgeReplicas {
		return ctx.Send(CacheFailed{error: xerrors.New("node cache failed")})
	}

	return nil
}

func (m *Manager) handleFinished(ctx statemachine.Context, info AssetCachingInfo) error {
	log.Debugf("handle asset finalize: %s", info.CID)
	m.removeAssetTicker(info.Hash.String())

	return nil
}

func (m *Manager) handleCachesFailed(ctx statemachine.Context, info AssetCachingInfo) error {
	m.removeAssetTicker(info.Hash.String())

	if info.RetryCount >= int64(MaxRetryCount) {
		log.Debugf("handle caches failed: %s reached the max retry count(%d), stop retrying", info.Hash, info.RetryCount)
		return nil
	}

	log.Debugf("handle caches failed: %s, retries: %d", info.Hash, info.RetryCount+1)

	if err := failedCoolDown(ctx, info); err != nil {
		return err
	}

	return ctx.Send(AssetReCache{})
}
