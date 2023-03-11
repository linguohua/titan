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
	// TODO: Exponential backoff when we see consecutive failures

	retryStart := time.Now().Add(MinRetryTime)
	if time.Now().Before(retryStart) {
		log.Infof("%s(%s), waiting %s before retrying", carfile.State, carfile.CarfileHash, time.Until(retryStart))
		select {
		case <-time.After(time.Until(retryStart)):
		case <-ctx.Context().Done():
			return ctx.Context().Err()
		}
	}

	return nil
}

func (m *Manager) handleCacheSeed(ctx statemachine.Context, carfile CarfileInfo) error {
	log.Infof("handle cache seed: %s", carfile.CarfileCID)

	// find nodes
	nodes := m.findCandidates(rootCachesCount, nil)
	if len(nodes) < 1 {
		return ctx.Send(CacheFailed{error: xerrors.New("node not found")})
	}

	// save to db
	err := m.saveCandidateReplicaInfos(nodes, carfile.CarfileHash.String())
	if err != nil {
		return ctx.Send(CacheFailed{error: err})
	}

	m.resetTimeoutTimer(carfile.CarfileHash.String())

	// send to nodes
	go func() {
		for _, node := range nodes {
			_, err := node.API().CacheCarfile(ctx.Context(), carfile.CarfileCID, nil)
			if err != nil {
				log.Errorf("%s CacheCarfile err:%s", node.NodeID, err.Error())
				continue
			}
		}
	}()

	return ctx.Send(CacheRequestSent{})
}

func (m *Manager) handleSeedCaching(ctx statemachine.Context, carfile CarfileInfo) error {
	log.Infof("handle get seed caching, %s", carfile.CarfileCID)

	if carfile.SucceedCandidateReplicas >= rootCachesCount {
		return ctx.Send(CacheSucceed{})
	}

	if carfile.SucceedCandidateReplicas+carfile.FailedCandidateReplicas >= rootCachesCount {
		return ctx.Send(CacheFailed{error: xerrors.New("node cache failed")})
	}

	return nil
}

func (m *Manager) handleCacheToCandidates(ctx statemachine.Context, carfile CarfileInfo) error {
	log.Infof("handle cache to candidates, %s", carfile.CarfileCID)

	needCount := carfile.CandidateReplicas - carfile.SucceedCandidateReplicas
	if needCount < 1 {
		// cache to edges
		return ctx.Send(CacheSucceed{})
	}

	filterNodes, err := m.nodeManager.CarfileDB.CandidatesByHash(carfile.CarfileHash.String())
	if err != nil {
		return ctx.Send(CacheFailed{error: err})
	}

	// find nodes
	nodes := m.findCandidates(int(needCount), filterNodes)
	if len(nodes) < 1 {
		return ctx.Send(CacheFailed{error: xerrors.New("node not found")})
	}

	// save to db
	err = m.saveCandidateReplicaInfos(nodes, carfile.CarfileHash.String())
	if err != nil {
		return ctx.Send(CacheFailed{error: err})
	}

	sources := m.Sources(carfile.CarfileHash.String(), filterNodes)

	m.resetTimeoutTimer(carfile.CarfileHash.String())

	// send to nodes
	go func() {
		for _, node := range nodes {
			_, err := node.API().CacheCarfile(ctx.Context(), carfile.CarfileCID, sources)
			if err != nil {
				log.Errorf("%s CacheCarfile err:%s", node.NodeID, err.Error())
				continue
			}
		}
	}()

	return ctx.Send(CacheRequestSent{})
}

func (m *Manager) handleCandidatesCaching(ctx statemachine.Context, carfile CarfileInfo) error {
	log.Infof("handle candidates caching, %s", carfile.CarfileCID)

	if carfile.SucceedCandidateReplicas >= carfile.CandidateReplicas {
		return ctx.Send(CacheSucceed{})
	}

	if carfile.SucceedCandidateReplicas+carfile.FailedCandidateReplicas >= carfile.CandidateReplicas {
		return ctx.Send(CacheFailed{error: xerrors.New("node cache failed")})
	}

	return nil
}

func (m *Manager) handleCacheToEdges(ctx statemachine.Context, carfile CarfileInfo) error {
	log.Infof("handle cache to edges , %s", carfile.CarfileCID)

	needCount := carfile.EdgeReplicas - carfile.SucceedEdgeReplicas
	cNodes, err := m.nodeManager.CarfileDB.CandidatesByHash(carfile.CarfileHash.String())
	if err != nil {
		return ctx.Send(CacheFailed{error: err})
	}

	sources := m.Sources(carfile.CarfileHash.String(), cNodes)
	if len(sources) < 1 {
		return ctx.Send(CacheFailed{error: xerrors.New("source node not found")})
	}

	filterNodes, err := m.nodeManager.CarfileDB.EdgesByHash(carfile.CarfileHash.String())
	if err != nil {
		return ctx.Send(CacheFailed{error: err})
	}

	// find nodes
	nodes := m.findEdges(int(needCount), filterNodes)
	if len(nodes) < 1 {
		return ctx.Send(CacheFailed{error: xerrors.New("node not found")})
	}

	// save to db
	err = m.saveEdgeReplicaInfos(nodes, carfile.CarfileHash.String())
	if err != nil {
		return ctx.Send(CacheFailed{error: err})
	}

	m.resetTimeoutTimer(carfile.CarfileHash.String())

	// send to nodes
	go func() {
		for _, node := range nodes {
			_, err := node.API().CacheCarfile(ctx.Context(), carfile.CarfileCID, sources)
			if err != nil {
				log.Errorf("%s CacheCarfile err:%s", node.NodeID, err.Error())
				continue
			}
		}
	}()

	return ctx.Send(CacheRequestSent{})
}

func (m *Manager) handleEdgesCaching(ctx statemachine.Context, carfile CarfileInfo) error {
	log.Infof("handle edge caching, %s", carfile.CarfileCID)
	if carfile.SucceedEdgeReplicas >= carfile.EdgeReplicas {
		return ctx.Send(CacheSucceed{})
	}

	if carfile.SucceedEdgeReplicas+carfile.FailedEdgeReplicas >= carfile.EdgeReplicas {
		return ctx.Send(CacheFailed{error: xerrors.New("node cache failed")})
	}

	return nil
}

func (m *Manager) handleFinalize(ctx statemachine.Context, carfile CarfileInfo) error {
	log.Infof("handle carfile finalize: %s", carfile.CarfileCID)

	m.stopTimeoutTimer(carfile.CarfileHash.String())

	return nil
}

func (m *Manager) handleCachesFailed(ctx statemachine.Context, carfile CarfileInfo) error {
	log.Infof("handle caches failed: %s", carfile.CarfileCID)

	if err := failedCooldown(ctx, carfile); err != nil {
		return err
	}
	return ctx.Send(CarfileRecache{})
}
