package storage

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

func failedCooldown(ctx statemachine.Context, carfile CarfileInfo) error {
	// TODO: Exponential back off when we see consecutive failures

	retryStart := time.Now().Add(MinRetryTime)
	if time.Now().Before(retryStart) {
		log.Debugf("%s(%s), waiting %s before retrying", carfile.State, carfile.CarfileHash, time.Until(retryStart))
		select {
		case <-time.After(time.Until(retryStart)):
		case <-ctx.Context().Done():
			return ctx.Context().Err()
		}
	}

	return nil
}

func (m *Manager) handleCacheSeed(ctx statemachine.Context, carfile CarfileInfo) error {
	log.Debugf("handle cache seed: %s", carfile.CarfileCID)

	// find nodes
	nodes := m.findCandidates(seedCacheCount, carfile.CandidateReplicaSucceeds)
	if len(nodes) < 1 {
		return ctx.Send(CacheFailed{error: xerrors.New("node not found")})
	}

	// save to db
	err := m.saveCandidateReplicaInfos(nodes, carfile.CarfileHash.String())
	if err != nil {
		return ctx.Send(CacheFailed{error: err})
	}

	m.addOrResetCarfileTicker(carfile.CarfileHash.String())

	// send to nodes
	go func() {
		for _, node := range nodes {
			err := node.API().CacheCarfile(ctx.Context(), carfile.CarfileCID, nil)
			if err != nil {
				log.Errorf("%s CacheCarfile err:%s", node.NodeID, err.Error())
				continue
			}
		}
	}()

	return ctx.Send(CacheRequestSent{})
}

func (m *Manager) handleSeedCaching(ctx statemachine.Context, carfile CarfileInfo) error {
	log.Debugf("handle seed caching, %s", carfile.CarfileCID)

	if len(carfile.CandidateReplicaSucceeds) >= seedCacheCount {
		return ctx.Send(CacheSucceed{})
	}

	if len(carfile.CandidateReplicaSucceeds)+len(carfile.CandidateReplicaFailures) >= seedCacheCount {
		return ctx.Send(CacheFailed{error: xerrors.New("node cache failed")})
	}

	return nil
}

func (m *Manager) handleCacheToCandidates(ctx statemachine.Context, carfile CarfileInfo) error {
	log.Debugf("handle cache to candidates, %s", carfile.CarfileCID)

	needCount := carfile.CandidateReplicas - int64(len(carfile.CandidateReplicaSucceeds))
	if needCount < 1 {
		// cache to edges
		return ctx.Send(CacheSucceed{})
	}

	// find nodes
	nodes := m.findCandidates(int(needCount), carfile.CandidateReplicaSucceeds)
	if len(nodes) < 1 {
		return ctx.Send(CacheFailed{error: xerrors.New("node not found")})
	}

	// save to db
	err := m.saveCandidateReplicaInfos(nodes, carfile.CarfileHash.String())
	if err != nil {
		return ctx.Send(CacheFailed{error: err})
	}

	sources := m.Sources(carfile.CarfileHash.String(), carfile.CandidateReplicaSucceeds)

	m.addOrResetCarfileTicker(carfile.CarfileHash.String())

	// send to nodes
	go func() {
		for _, node := range nodes {
			err := node.API().CacheCarfile(ctx.Context(), carfile.CarfileCID, sources)
			if err != nil {
				log.Errorf("%s CacheCarfile err:%s", node.NodeID, err.Error())
				continue
			}
		}
	}()

	return ctx.Send(CacheRequestSent{})
}

func (m *Manager) handleCandidatesCaching(ctx statemachine.Context, carfile CarfileInfo) error {
	log.Debugf("handle candidates caching, %s", carfile.CarfileCID)

	if int64(len(carfile.CandidateReplicaSucceeds)) >= carfile.CandidateReplicas {
		return ctx.Send(CacheSucceed{})
	}

	if int64(len(carfile.CandidateReplicaSucceeds)+len(carfile.CandidateReplicaFailures)) >= carfile.CandidateReplicas {
		return ctx.Send(CacheFailed{error: xerrors.New("node cache failed")})
	}

	return nil
}

func (m *Manager) handleCacheToEdges(ctx statemachine.Context, carfile CarfileInfo) error {
	log.Debugf("handle cache to edges , %s", carfile.CarfileCID)

	needCount := carfile.EdgeReplicas - int64(len(carfile.EdgeReplicaSucceeds))
	if needCount < 1 {
		return ctx.Send(CacheSucceed{})
	}

	sources := m.Sources(carfile.CarfileHash.String(), carfile.CandidateReplicaSucceeds)
	if len(sources) < 1 {
		return ctx.Send(CacheFailed{error: xerrors.New("source node not found")})
	}

	// find nodes
	nodes := m.findEdges(int(needCount), carfile.EdgeReplicaSucceeds)
	if len(nodes) < 1 {
		return ctx.Send(CacheFailed{error: xerrors.New("node not found")})
	}

	// save to db
	err := m.saveEdgeReplicaInfos(nodes, carfile.CarfileHash.String())
	if err != nil {
		return ctx.Send(CacheFailed{error: err})
	}

	m.addOrResetCarfileTicker(carfile.CarfileHash.String())

	// send to nodes
	go func() {
		for _, node := range nodes {
			err := node.API().CacheCarfile(ctx.Context(), carfile.CarfileCID, sources)
			if err != nil {
				log.Errorf("%s CacheCarfile err:%s", node.NodeID, err.Error())
				continue
			}
		}
	}()

	return ctx.Send(CacheRequestSent{})
}

func (m *Manager) handleEdgesCaching(ctx statemachine.Context, carfile CarfileInfo) error {
	log.Debugf("handle edge caching, %s", carfile.CarfileCID)
	if int64(len(carfile.EdgeReplicaSucceeds)) >= carfile.EdgeReplicas {
		return ctx.Send(CacheSucceed{})
	}

	if int64(len(carfile.EdgeReplicaSucceeds)+len(carfile.EdgeReplicaFailures)) >= carfile.EdgeReplicas {
		return ctx.Send(CacheFailed{error: xerrors.New("node cache failed")})
	}

	return nil
}

func (m *Manager) handleFinalize(ctx statemachine.Context, carfile CarfileInfo) error {
	log.Debugf("handle carfile finalize: %s", carfile.CarfileCID)
	defer m.removeCarfileTicker(carfile.CarfileHash.String())

	return nil
}

func (m *Manager) handleCachesFailed(ctx statemachine.Context, carfile CarfileInfo) error {
	if carfile.RetryCount >= int64(MaxRetryCount) {
		log.Debugf("handle caches failed: %s reached the max retry count(%d), stop retrying", carfile.CarfileHash, carfile.RetryCount)
		return nil
	}

	log.Debugf("handle caches failed: %s, retries: %d", carfile.CarfileHash, carfile.RetryCount+1)

	defer m.removeCarfileTicker(carfile.CarfileHash.String())

	if err := failedCooldown(ctx, carfile); err != nil {
		return err
	}

	return ctx.Send(CarfileReCache{})
}
