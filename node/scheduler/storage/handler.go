package storage

import (
	"time"

	"github.com/filecoin-project/go-statemachine"
	"github.com/linguohua/titan/api/types"
	"golang.org/x/xerrors"
)

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

func (m *Manager) handleStartCache(ctx statemachine.Context, carfile CarfileInfo) error {
	log.Infof("handle start cache: %s", carfile.CarfileCID)
	return ctx.Send(CarfileGetSeed{})
}

func (m *Manager) handleGetSeed(ctx statemachine.Context, carfile CarfileInfo) error {
	log.Infof("handle get seed: %s", carfile.CarfileCID)

	// find nodes
	nodes := m.findCandidates(rootCacheCount, nil)
	if len(nodes) < 1 {
		return ctx.Send(CarfileCacheFailed{error: xerrors.New("not found node")})
	}

	// save to db
	err := m.saveCandidateReplicaInfos(nodes, carfile.CarfileHash.String())
	if err != nil {
		return ctx.Send(CarfileCacheFailed{error: err})
	}

	// send to nodes
	for _, node := range nodes {
		_, err := node.API().CacheCarfile(ctx.Context(), carfile.CarfileCID, nil)
		if err != nil {
			log.Errorf("%s CacheCarfile err:%s", node.NodeID, err.Error())
			continue
		}
	}

	return nil
}

func (m *Manager) handleGetSeedCompleted(ctx statemachine.Context, carfile CarfileInfo) error {
	log.Infof("handle get seed completed: %s", carfile.CarfileCID)

	err := m.nodeManager.CarfileDB.UpdateCarfileRecordCachesInfo(&types.CarfileRecordInfo{
		CarfileHash: carfile.CarfileHash.String(),
		TotalBlocks: int(carfile.Blocks),
		TotalSize:   carfile.Size,
	})
	if err != nil {
		return err
	}
	// send next status
	return ctx.Send(CarfileCandidateCaching{})
}

func (m *Manager) handleCandidateCaching(ctx statemachine.Context, carfile CarfileInfo) error {
	log.Infof("handle candidate caching: %s", carfile.CarfileCID)
	if candidateReplicaCacheCount < 1 {
		// cache to edges
		return ctx.Send(CarfileCacheCompleted{})
	}

	filterNodes, err := m.nodeManager.CarfileDB.CandidatesWithHash(carfile.CarfileHash.String())
	if err != nil {
		return ctx.Send(CarfileCacheFailed{error: err})
	}

	// find nodes
	nodes := m.findCandidates(candidateReplicaCacheCount, filterNodes)
	if len(nodes) < 1 {
		return ctx.Send(CarfileCacheFailed{error: xerrors.New("not found node")})
	}

	// save to db
	err = m.saveCandidateReplicaInfos(nodes, carfile.CarfileHash.String())
	if err != nil {
		return ctx.Send(CarfileCacheFailed{error: err})
	}

	sources := m.Sources(carfile.CarfileHash.String(), filterNodes)

	// send to nodes
	for _, node := range nodes {
		_, err := node.API().CacheCarfile(ctx.Context(), carfile.CarfileCID, sources)
		if err != nil {
			log.Errorf("%s CacheCarfile err:%s", node.NodeID, err.Error())
			continue
		}
	}

	return nil
}

func (m *Manager) handleCandidateCacheCompleted(ctx statemachine.Context, carfile CarfileInfo) error {
	log.Infof("handle candidate cache completed: %s", carfile.CarfileCID)

	// all candidate cache completed
	nodes, err := m.nodeManager.CarfileDB.CandidatesWithHash(carfile.CarfileHash.String())
	if err != nil {
		return ctx.Send(CarfileCacheFailed{error: err})
	}

	if int64(len(nodes)) == carfile.CandidateReplicas {
		return ctx.Send(CarfileEdgeCaching{})
	}

	return nil
}

func (m *Manager) handleEdgeCaching(ctx statemachine.Context, carfile CarfileInfo) error {
	log.Infof("handle edge caching: %s", carfile.CarfileCID)

	filterNodes, err := m.nodeManager.CarfileDB.EdgesWithHash(carfile.CarfileHash.String())
	if err != nil {
		return ctx.Send(CarfileCacheFailed{error: err})
	}

	// find nodes
	nodes := m.findEdges(int(carfile.EdgeReplicas), filterNodes)
	if len(nodes) < 1 {
		return ctx.Send(CarfileCacheFailed{error: xerrors.New("not found node")})
	}

	// save to db
	err = m.saveEdgeReplicaInfos(nodes, carfile.CarfileHash.String())
	if err != nil {
		return ctx.Send(CarfileCacheFailed{error: err})
	}

	cNdoes, err := m.nodeManager.CarfileDB.CandidatesWithHash(carfile.CarfileHash.String())
	if err != nil {
		return ctx.Send(CarfileCacheFailed{error: err})
	}

	sources := m.Sources(carfile.CarfileHash.String(), cNdoes)

	// send to nodes
	for _, node := range nodes {
		_, err := node.API().CacheCarfile(ctx.Context(), carfile.CarfileCID, sources)
		if err != nil {
			log.Errorf("%s CacheCarfile err:%s", node.NodeID, err.Error())
			continue
		}
	}

	return nil
}

func (m *Manager) handleEdgeCacheCompleted(ctx statemachine.Context, carfile CarfileInfo) error {
	log.Infof("handle edge cache completed: %s", carfile.CarfileCID)

	// all edges cache completed
	nodes, err := m.nodeManager.CarfileDB.EdgesWithHash(carfile.CarfileHash.String())
	if err != nil {
		return ctx.Send(CarfileCacheFailed{error: err})
	}

	if int64(len(nodes)) == carfile.EdgeReplicas {
		return ctx.Send(CarfileFinalize{})
	}

	return nil
}

func (m *Manager) handleFinalize(ctx statemachine.Context, carfile CarfileInfo) error {
	log.Infof("handle carfile finalize: %s", carfile.CarfileCID)
	return nil
}

func (m *Manager) handleGetSeedFailed(ctx statemachine.Context, carfile CarfileInfo) error {
	log.Infof("handle get seed failed: %s", carfile.CarfileCID)
	if err := failedCooldown(ctx, carfile); err != nil {
		return err
	}
	return ctx.Send(CarfileGetSeed{})
}

func (m *Manager) handleCandidateCachingFailed(ctx statemachine.Context, carfile CarfileInfo) error {
	log.Infof("handle candidate cache failed: %s", carfile.CarfileCID)
	if err := failedCooldown(ctx, carfile); err != nil {
		return err
	}
	return ctx.Send(CarfileCandidateCaching{})
}

func (m *Manager) handleEdgeCachingFailed(ctx statemachine.Context, carfile CarfileInfo) error {
	log.Infof("handle edge cache failed: %s", carfile.CarfileCID)
	if err := failedCooldown(ctx, carfile); err != nil {
		return err
	}
	return ctx.Send(CarfileEdgeCaching{})
}
