package storage

import (
	"github.com/filecoin-project/go-statemachine"
	"github.com/linguohua/titan/api/types"
)

func (m *Manager) handleStartCache(ctx statemachine.Context, carfile CarfileInfo) error {
	log.Info("handler statr cache, %s", carfile.CarfileCID)
	return ctx.Send(CarfileGetSeed{})
}

func (m *Manager) handleGetSeed(ctx statemachine.Context, carfile CarfileInfo) error {
	log.Info("handler get seed file, %s", carfile.CarfileCID)

	// find nodes
	nodes := m.findCandidates(rootCacheCount, carfile.completedCandidateReplicas)
	if len(nodes) < 1 {
		return ctx.Send(CarfileCacheFailed{})
	}

	// save to db
	err := m.saveCandidateReplicaInfos(nodes, carfile.CarfileHash)
	if err != nil {
		return ctx.Send(CarfileCacheFailed{})
	}

	// send to nodes
	for _, node := range nodes {
		_, err := node.API().CacheCarfile(ctx.Context(), carfile.CarfileCID.String(), nil)
		if err != nil {
			log.Errorf("%s CacheCarfile err:%s", node.NodeID, err.Error())
			continue
		}
	}

	return nil
}

func (m *Manager) handleGetSeedCompleted(ctx statemachine.Context, carfile CarfileInfo) error {
	log.Info("handler get seed completed, %s", carfile.CarfileCID)

	// save to db
	cInfo := &types.ReplicaInfo{
		ID:     replicaID(carfile.CarfileHash, carfile.lastResultInfo.NodeID),
		NodeID: carfile.lastResultInfo.NodeID,
		Status: carfile.lastResultInfo.Status,
	}
	err := m.nodeManager.CarfileDB.UpdateCarfileReplicaInfo(cInfo)
	if err != nil {
		return err
	}

	// send next status
	return ctx.Send(CarfileCandidateCaching{})
}

func (m *Manager) handleCandidateCaching(ctx statemachine.Context, carfile CarfileInfo) error {
	log.Info("handler candidate  Caching, %s", carfile.CarfileCID)
	if candidateReplicaCacheCount < 1 {
		// cache to edges
		return ctx.Send(CarfileCacheCompleted{})
	}

	// find nodes
	nodes := m.findCandidates(candidateReplicaCacheCount, carfile.completedCandidateReplicas)
	if len(nodes) < 1 {
		return ctx.Send(CarfileCacheFailed{})
	}

	// save to db
	err := m.saveCandidateReplicaInfos(nodes, carfile.CarfileHash)
	if err != nil {
		return ctx.Send(CarfileCacheFailed{})
	}

	// send to nodes
	for _, node := range nodes {
		_, err := node.API().CacheCarfile(ctx.Context(), carfile.CarfileCID.String(), carfile.downloadSources)
		if err != nil {
			log.Errorf("%s CacheCarfile err:%s", node.NodeID, err.Error())
			continue
		}
	}

	return nil
}

func (m *Manager) handleCandidatesCacheCompleted(ctx statemachine.Context, carfile CarfileInfo) error {
	log.Info("handler candidates cache completed, %s", carfile.CarfileCID)

	// save to db
	cInfo := &types.ReplicaInfo{
		ID:     replicaID(carfile.CarfileHash, carfile.lastResultInfo.NodeID),
		NodeID: carfile.lastResultInfo.NodeID,
		Status: carfile.lastResultInfo.Status,
	}
	err := m.nodeManager.CarfileDB.UpdateCarfileReplicaInfo(cInfo)
	if err != nil {
		return err
	}

	// all candidate cache completed
	if len(carfile.completedCandidateReplicas) == carfile.candidateReplicas {
		return ctx.Send(CarfileEdgeCaching{})
	}

	return nil
}

func (m *Manager) handleEdgeCaching(ctx statemachine.Context, carfile CarfileInfo) error {
	log.Info("handler carfile , %s", carfile.CarfileCID)

	// find nodes
	nodes := m.findEdges(candidateReplicaCacheCount, carfile.completedCandidateReplicas)
	if len(nodes) < 1 {
		return ctx.Send(CarfileCacheFailed{})
	}

	// save to db
	err := m.saveEdgeReplicaInfos(nodes, carfile.CarfileHash)
	if err != nil {
		return ctx.Send(CarfileCacheFailed{})
	}

	// send to nodes
	for _, node := range nodes {
		_, err := node.API().CacheCarfile(ctx.Context(), carfile.CarfileCID.String(), carfile.downloadSources)
		if err != nil {
			log.Errorf("%s CacheCarfile err:%s", node.NodeID, err.Error())
			continue
		}
	}

	return nil
}

func (m *Manager) handleEdgeCacheCompleted(ctx statemachine.Context, carfile CarfileInfo) error {
	log.Info("handler edge cache completed, %s", carfile.CarfileCID)

	// save to db
	cInfo := &types.ReplicaInfo{
		ID:          replicaID(carfile.CarfileHash, carfile.lastResultInfo.NodeID),
		NodeID:      carfile.lastResultInfo.NodeID,
		Status:      carfile.lastResultInfo.Status,
		IsCandidate: false,
	}
	err := m.nodeManager.CarfileDB.UpdateCarfileReplicaInfo(cInfo)
	if err != nil {
		return err
	}

	// all candidate cache completed
	if len(carfile.completedEdgeReplicas) == carfile.Replicas {
		return ctx.Send(CarfileFinalize{})
	}

	return nil
}

func (m *Manager) handleFinalize(ctx statemachine.Context, carfile CarfileInfo) error {
	log.Info("handler carfile finalize, %s", carfile.CarfileCID)
	return nil
}

func (m *Manager) handleGetSeedFailed(ctx statemachine.Context, carfile CarfileInfo) error {
	log.Info("handler get carfile failed, %s", carfile.CarfileCID)
	return ctx.Send(CarfileGetSeed{})
}

func (m *Manager) handleCandidateCachingFailed(ctx statemachine.Context, carfile CarfileInfo) error {
	log.Info("handler candidate  Caching failed, %s", carfile.CarfileCID)
	return ctx.Send(CarfileCandidateCaching{})
}

func (m *Manager) handlerEdgeCachingFailed(ctx statemachine.Context, carfile CarfileInfo) error {
	log.Info("handler edge  Caching failed, %s", carfile.CarfileCID)
	return ctx.Send(CarfileEdgeCaching{})
}
