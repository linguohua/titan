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

func failedCoolDown(ctx statemachine.Context, info AssetPullingInfo) error {
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

func (m *Manager) handleSeedSelect(ctx statemachine.Context, info AssetPullingInfo) error {
	log.Debugf("handle select seed: %s", info.CID)

	if len(info.CandidateReplicaSucceeds) >= seedReplicaCount {
		// The number of candidate node replicas has reached the requirement
		return ctx.Send(Skip{})
	}

	// find nodes
	nodes := m.nodeMgr.SelectCandidateToPullAsset(seedReplicaCount, info.CandidateReplicaSucceeds)
	if len(nodes) < 1 {
		return ctx.Send(SelectFailed{error: xerrors.New("node not found")})
	}

	// save to db
	err := m.saveReplicaInfos(nodes, info.Hash.String(), true)
	if err != nil {
		return ctx.Send(SelectFailed{error: err})
	}

	m.addOrResetAssetTicker(info.Hash.String())

	// send a cache request to the node
	go func() {
		for _, node := range nodes {
			err := node.CacheCarfile(ctx.Context(), info.CID, nil)
			if err != nil {
				log.Errorf("%s pull asset err:%s", node.NodeInfo.NodeID, err.Error())
				continue
			}

			node.IncrCurPullingCount(1)
		}
	}()

	return ctx.Send(PullRequestSent{})
}

func (m *Manager) handleSeedPulling(ctx statemachine.Context, info AssetPullingInfo) error {
	log.Debugf("handle seed pulling, %s", info.CID)

	if len(info.CandidateReplicaSucceeds) >= seedReplicaCount {
		return ctx.Send(PullSucceed{})
	}

	if len(info.CandidateReplicaSucceeds)+len(info.CandidateReplicaFailures) >= seedReplicaCount {
		return ctx.Send(PullFailed{error: xerrors.New("node pull failed")})
	}

	return nil
}

func (m *Manager) handleCandidatesSelect(ctx statemachine.Context, info AssetPullingInfo) error {
	log.Debugf("handle select candidates, %s", info.CID)

	needCount := info.CandidateReplicas - int64(len(info.CandidateReplicaSucceeds))
	if needCount < 1 {
		// The number of candidate node replicas has reached the requirement
		return ctx.Send(Skip{})
	}

	// find nodes
	nodes := m.nodeMgr.SelectCandidateToPullAsset(int(needCount), info.CandidateReplicaSucceeds)
	if len(nodes) < 1 {
		return ctx.Send(SelectFailed{error: xerrors.New("node not found")})
	}

	// save to db
	err := m.saveReplicaInfos(nodes, info.Hash.String(), true)
	if err != nil {
		return ctx.Send(SelectFailed{error: err})
	}

	sources := m.Sources(info.Hash.String(), info.CandidateReplicaSucceeds)

	m.addOrResetAssetTicker(info.Hash.String())

	// send a pull request to the node
	go func() {
		for _, node := range nodes {
			err := node.CacheCarfile(ctx.Context(), info.CID, sources)
			if err != nil {
				log.Errorf("%s pull asset err:%s", node.NodeInfo.NodeID, err.Error())
				continue
			}

			node.IncrCurPullingCount(1)
		}
	}()

	return ctx.Send(PullRequestSent{})
}

func (m *Manager) handleCandidatesPulling(ctx statemachine.Context, info AssetPullingInfo) error {
	log.Debugf("handle candidates pulling, %s", info.CID)

	if int64(len(info.CandidateReplicaSucceeds)) >= info.CandidateReplicas {
		return ctx.Send(PullSucceed{})
	}

	if int64(len(info.CandidateReplicaSucceeds)+len(info.CandidateReplicaFailures)) >= info.CandidateReplicas {
		return ctx.Send(PullFailed{error: xerrors.New("node pull failed")})
	}

	return nil
}

func (m *Manager) handleEdgesSelect(ctx statemachine.Context, info AssetPullingInfo) error {
	log.Debugf("handle select edges , %s", info.CID)

	needCount := info.EdgeReplicas - int64(len(info.EdgeReplicaSucceeds))
	if needCount < 1 {
		// The number of edge node replicas has reached the requirement
		return ctx.Send(Skip{})
	}

	sources := m.Sources(info.Hash.String(), info.CandidateReplicaSucceeds)
	if len(sources) < 1 {
		return ctx.Send(SelectFailed{error: xerrors.New("source node not found")})
	}

	// find nodes
	nodes := m.nodeMgr.SelectEdgeToPullAsset(int(needCount), info.EdgeReplicaSucceeds)
	if len(nodes) < 1 {
		return ctx.Send(SelectFailed{error: xerrors.New("node not found")})
	}

	// save to db
	err := m.saveReplicaInfos(nodes, info.Hash.String(), false)
	if err != nil {
		return ctx.Send(SelectFailed{error: err})
	}

	m.addOrResetAssetTicker(info.Hash.String())

	// send a pull request to the node
	go func() {
		for _, node := range nodes {
			err := node.CacheCarfile(ctx.Context(), info.CID, sources)
			if err != nil {
				log.Errorf("%s pull asset err:%s", node.NodeInfo.NodeID, err.Error())
				continue
			}

			node.IncrCurPullingCount(1)
		}
	}()

	return ctx.Send(PullRequestSent{})
}

func (m *Manager) handleEdgesPulling(ctx statemachine.Context, info AssetPullingInfo) error {
	log.Debugf("handle edge pulling, %s", info.CID)
	if int64(len(info.EdgeReplicaSucceeds)) >= info.EdgeReplicas {
		return ctx.Send(PullSucceed{})
	}

	if int64(len(info.EdgeReplicaSucceeds)+len(info.EdgeReplicaFailures)) >= info.EdgeReplicas {
		return ctx.Send(PullFailed{error: xerrors.New("node pull failed")})
	}

	return nil
}

func (m *Manager) handleServicing(ctx statemachine.Context, info AssetPullingInfo) error {
	log.Infof("handle asset servicing: %s", info.CID)
	m.removeAssetTicker(info.Hash.String())

	return nil
}

func (m *Manager) handlePullsFailed(ctx statemachine.Context, info AssetPullingInfo) error {
	m.removeAssetTicker(info.Hash.String())

	if info.RetryCount >= int64(MaxRetryCount) {
		log.Infof("handle pulls failed: %s, retry count: %d", info.CID, info.RetryCount)
		return nil
	}

	log.Debugf("handle pulls failed: %s, retries: %d", info.CID, info.RetryCount+1)

	if err := failedCoolDown(ctx, info); err != nil {
		return err
	}

	return ctx.Send(AssetRePull{})
}
