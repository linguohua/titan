package storage

import (
	"time"

	"github.com/filecoin-project/go-statemachine"
	"golang.org/x/xerrors"
)

// MinRetryTime retry time
var MinRetryTime = 1 * time.Minute

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

func (m *Manager) handleGetSeed(ctx statemachine.Context, carfile CarfileInfo) error {
	log.Infof("handle get seed: %s", carfile.CarfileCID)

	// find nodes
	nodes := m.findCandidates(rootCacheCount, nil)
	if len(nodes) < 1 {
		return ctx.Send(CacheFailed{error: xerrors.New("not found node")})
	}

	// save to db
	err := m.saveCandidateReplicaInfos(nodes, carfile.CarfileHash.String())
	if err != nil {
		return ctx.Send(CacheFailed{error: err})
	}

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

	return ctx.Send(RequestNodes{})
}

func (m *Manager) handleGetSeedCaching(ctx statemachine.Context, carfile CarfileInfo) error {
	log.Infof("handler get seed caching, %s", carfile.CarfileCID)

	log.Warnf("carfile.SuccessedCandidateReplicas : %d", carfile.SuccessedCandidateReplicas)
	if carfile.SuccessedCandidateReplicas >= rootCacheCount {
		return ctx.Send(CacheSuccessed{})
	}

	if carfile.SuccessedCandidateReplicas+carfile.FailedCandidateReplicas >= rootCacheCount {
		return ctx.Send(CacheFailed{error: xerrors.New("node cache fail")})
	}

	return nil
}

func (m *Manager) handleStartCandidatesCache(ctx statemachine.Context, carfile CarfileInfo) error {
	log.Infof("handler start candidates cache, %s", carfile.CarfileCID)

	needCount := carfile.CandidateReplicas - carfile.SuccessedCandidateReplicas
	if needCount < 1 {
		// cache to edges
		return ctx.Send(CacheSuccessed{})
	}

	filterNodes, err := m.nodeManager.CarfileDB.CandidatesWithHash(carfile.CarfileHash.String())
	if err != nil {
		return ctx.Send(CacheFailed{error: err})
	}

	// find nodes
	nodes := m.findCandidates(int(needCount), filterNodes)
	if len(nodes) < 1 {
		return ctx.Send(CacheFailed{error: xerrors.New("not found node")})
	}

	// save to db
	err = m.saveCandidateReplicaInfos(nodes, carfile.CarfileHash.String())
	if err != nil {
		return ctx.Send(CacheFailed{error: err})
	}

	sources := m.Sources(carfile.CarfileHash.String(), filterNodes)

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

	return ctx.Send(RequestNodes{})
}

func (m *Manager) handleCandidatesCaching(ctx statemachine.Context, carfile CarfileInfo) error {
	log.Infof("handler candidates caching, %s", carfile.CarfileCID)

	if carfile.SuccessedCandidateReplicas >= carfile.CandidateReplicas {
		return ctx.Send(CacheSuccessed{})
	}

	if carfile.SuccessedCandidateReplicas+carfile.FailedCandidateReplicas >= carfile.CandidateReplicas {
		return ctx.Send(CacheFailed{error: xerrors.New("node cache fail")})
	}

	return nil
}

func (m *Manager) handleStartEdgesCache(ctx statemachine.Context, carfile CarfileInfo) error {
	log.Infof("handler start edges cache , %s", carfile.CarfileCID)

	needCount := carfile.EdgeReplicas - carfile.SuccessedEdgeReplicas

	cNdoes, err := m.nodeManager.CarfileDB.CandidatesWithHash(carfile.CarfileHash.String())
	if err != nil {
		return ctx.Send(CacheFailed{error: err})
	}

	sources := m.Sources(carfile.CarfileHash.String(), cNdoes)
	if len(sources) < 1 {
		return ctx.Send(CacheFailed{error: xerrors.New("not found source nodes")})
	}

	filterNodes, err := m.nodeManager.CarfileDB.EdgesWithHash(carfile.CarfileHash.String())
	if err != nil {
		return ctx.Send(CacheFailed{error: err})
	}

	// find nodes
	nodes := m.findEdges(int(needCount), filterNodes)
	if len(nodes) < 1 {
		return ctx.Send(CacheFailed{error: xerrors.New("not found node")})
	}

	// save to db
	err = m.saveEdgeReplicaInfos(nodes, carfile.CarfileHash.String())
	if err != nil {
		return ctx.Send(CacheFailed{error: err})
	}

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

	return ctx.Send(RequestNodes{})
}

func (m *Manager) handleEdgesCaching(ctx statemachine.Context, carfile CarfileInfo) error {
	log.Infof("handler edge caching, %s", carfile.CarfileCID)

	if carfile.SuccessedEdgeReplicas >= carfile.EdgeReplicas {
		return ctx.Send(CacheSuccessed{})
	}

	if carfile.SuccessedEdgeReplicas+carfile.FailedEdgeReplicas >= carfile.EdgeReplicas {
		return ctx.Send(CacheFailed{error: xerrors.New("node cache fail")})
	}

	return nil
}

func (m *Manager) handleFinalize(ctx statemachine.Context, carfile CarfileInfo) error {
	log.Infof("handle carfile finalize: %s", carfile.CarfileCID)
	return nil
}

func (m *Manager) handleCachesFailed(ctx statemachine.Context, carfile CarfileInfo) error {
	log.Infof("handle caches failed: %s", carfile.CarfileCID)
	if err := failedCooldown(ctx, carfile); err != nil {
		return err
	}
	return ctx.Send(CarfileRecache{})
}

// func (m *Manager) handleCandidateCachingFailed(ctx statemachine.Context, carfile CarfileInfo) error {
// 	log.Infof("handle candidate cache failed: %s", carfile.CarfileCID)
// 	if err := failedCooldown(ctx, carfile); err != nil {
// 		return err
// 	}
// 	return ctx.Send(CarfileRecache{})
// }

// func (m *Manager) handleEdgeCachingFailed(ctx statemachine.Context, carfile CarfileInfo) error {
// 	log.Infof("handle edge cache failed: %s", carfile.CarfileCID)
// 	if err := failedCooldown(ctx, carfile); err != nil {
// 		return err
// 	}
// 	return ctx.Send(CarfileRecache{})
// }
