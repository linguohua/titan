package storage

import (
	"github.com/filecoin-project/go-statemachine"
	"github.com/linguohua/titan/api/types"
	"time"
)

var MinRetryTime = 1 * time.Minute

func failedCooldown(ctx statemachine.Context, carfile CarfileInfo) error {
	// TODO: Exponential backoff when we see consecutive failures

	retryStart := time.Unix(int64(carfile.Log[len(carfile.Log)-1].Timestamp), 0).Add(MinRetryTime)
	if len(carfile.Log) > 0 && !time.Now().After(retryStart) {
		log.Infof("%s(%d), waiting %s before retrying", carfile.State, carfile.CarfileHash, time.Until(retryStart))
		select {
		case <-time.After(time.Until(retryStart)):
		case <-ctx.Context().Done():
			return ctx.Context().Err()
		}
	}

	return nil
}

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
	err := m.saveCandidateReplicaInfos(nodes, carfile.CarfileHash.String())
	if err != nil {
		return ctx.Send(CarfileCacheFailed{})
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
	log.Info("handler get seed completed, %s", carfile.CarfileCID)

	// save to db
	cInfo := &types.ReplicaInfo{
		ID:     replicaID(carfile.CarfileHash.String(), carfile.lastResultInfo.NodeID),
		NodeID: carfile.lastResultInfo.NodeID,
		Status: types.CacheStatus(carfile.lastResultInfo.Status),
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
	err := m.saveCandidateReplicaInfos(nodes, carfile.CarfileHash.String())
	if err != nil {
		return ctx.Send(CarfileCacheFailed{})
	}

	// send to nodes
	for _, node := range nodes {
		_, err := node.API().CacheCarfile(ctx.Context(), carfile.CarfileCID, carfile.downloadSources)
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
		ID:     replicaID(carfile.CarfileHash.String(), carfile.lastResultInfo.NodeID),
		NodeID: carfile.lastResultInfo.NodeID,
		Status: types.CacheStatus(carfile.lastResultInfo.Status),
	}
	err := m.nodeManager.CarfileDB.UpdateCarfileReplicaInfo(cInfo)
	if err != nil {
		return err
	}

	// all candidate cache completed
	if int64(len(carfile.completedCandidateReplicas)) == carfile.candidateReplicas {
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
	err := m.saveEdgeReplicaInfos(nodes, carfile.CarfileHash.String())
	if err != nil {
		return ctx.Send(CarfileCacheFailed{})
	}

	// send to nodes
	for _, node := range nodes {
		_, err := node.API().CacheCarfile(ctx.Context(), carfile.CarfileCID, carfile.downloadSources)
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
		ID:          replicaID(carfile.CarfileHash.String(), carfile.lastResultInfo.NodeID),
		NodeID:      carfile.lastResultInfo.NodeID,
		Status:      types.CacheStatus(carfile.lastResultInfo.Status),
		IsCandidate: false,
	}
	err := m.nodeManager.CarfileDB.UpdateCarfileReplicaInfo(cInfo)
	if err != nil {
		return err
	}

	// all candidate cache completed
	if int64(len(carfile.completedEdgeReplicas)) == carfile.Replicas {
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
	if err := failedCooldown(ctx, carfile); err != nil {
		return err
	}
	return ctx.Send(CarfileGetSeed{})
}

func (m *Manager) handleCandidateCachingFailed(ctx statemachine.Context, carfile CarfileInfo) error {
	log.Info("handler candidate  Caching failed, %s", carfile.CarfileCID)
	if err := failedCooldown(ctx, carfile); err != nil {
		return err
	}
	return ctx.Send(CarfileCandidateCaching{})
}

func (m *Manager) handlerEdgeCachingFailed(ctx statemachine.Context, carfile CarfileInfo) error {
	log.Info("handler edge  Caching failed, %s", carfile.CarfileCID)
	if err := failedCooldown(ctx, carfile); err != nil {
		return err
	}
	return ctx.Send(CarfileEdgeCaching{})
}
