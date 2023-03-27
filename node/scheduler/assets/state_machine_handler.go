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

func (m *Manager) handleFindFirstCandidate(ctx statemachine.Context, info AssetPullingInfo) error {
	log.Debugf("handle find first candidate: %s", info.CID)

	// find nodes
	nodes := m.findCandidates(seedReplicaCount, info.CandidateReplicaSucceeds)
	if len(nodes) < 1 {
		return ctx.Send(PullFailed{error: xerrors.New("node not found")})
	}

	// save to db
	err := m.saveCandidateReplicaInfos(nodes, info.Hash.String())
	if err != nil {
		return ctx.Send(PullFailed{error: err})
	}

	m.addOrResetAssetTicker(info.Hash.String())

	// send a cache request to the node
	go func() {
		for _, node := range nodes {
			err := node.API().CacheCarfile(ctx.Context(), info.CID, nil)
			if err != nil {
				log.Errorf("%s pull asset err:%s", node.NodeID, err.Error())
				continue
			}

			node.IncrCurPullingCount(1)
		}
	}()

	return ctx.Send(PullRequestSent{})
}

func (m *Manager) handleAssetSeedPulling(ctx statemachine.Context, info AssetPullingInfo) error {
	log.Debugf("handle seed pulling, %s", info.CID)

	if len(info.CandidateReplicaSucceeds) >= seedReplicaCount {
		return ctx.Send(PullSucceed{})
	}

	if len(info.CandidateReplicaSucceeds)+len(info.CandidateReplicaFailures) >= seedReplicaCount {
		return ctx.Send(PullFailed{error: xerrors.New("node pull failed")})
	}

	return nil
}

func (m *Manager) handleFindCandidates(ctx statemachine.Context, info AssetPullingInfo) error {
	log.Debugf("handle find candidates, %s", info.CID)

	needCount := info.CandidateReplicas - int64(len(info.CandidateReplicaSucceeds))
	if needCount < 1 {
		// The number of candidate node replicas has reached the requirement
		return ctx.Send(PullSucceed{})
	}

	// find nodes
	nodes := m.findCandidates(int(needCount), info.CandidateReplicaSucceeds)
	if len(nodes) < 1 {
		return ctx.Send(PullFailed{error: xerrors.New("node not found")})
	}

	// save to db
	err := m.saveCandidateReplicaInfos(nodes, info.Hash.String())
	if err != nil {
		return ctx.Send(PullFailed{error: err})
	}

	sources := m.Sources(info.Hash.String(), info.CandidateReplicaSucceeds)

	m.addOrResetAssetTicker(info.Hash.String())

	// send a pull request to the node
	go func() {
		for _, node := range nodes {
			err := node.API().CacheCarfile(ctx.Context(), info.CID, sources)
			if err != nil {
				log.Errorf("%s pull asset err:%s", node.NodeID, err.Error())
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

func (m *Manager) handleFindEdges(ctx statemachine.Context, info AssetPullingInfo) error {
	log.Debugf("handle find edges , %s", info.CID)

	needCount := info.EdgeReplicas - int64(len(info.EdgeReplicaSucceeds))
	if needCount < 1 {
		// The number of edge node replicas has reached the requirement
		return ctx.Send(PullSucceed{})
	}

	sources := m.Sources(info.Hash.String(), info.CandidateReplicaSucceeds)
	if len(sources) < 1 {
		return ctx.Send(PullFailed{error: xerrors.New("source node not found")})
	}

	// find nodes
	nodes := m.findEdges(int(needCount), info.EdgeReplicaSucceeds)
	if len(nodes) < 1 {
		return ctx.Send(PullFailed{error: xerrors.New("node not found")})
	}

	// save to db
	err := m.saveEdgeReplicaInfos(nodes, info.Hash.String())
	if err != nil {
		return ctx.Send(PullFailed{error: err})
	}

	m.addOrResetAssetTicker(info.Hash.String())

	// send a pull request to the node
	go func() {
		for _, node := range nodes {
			err := node.API().CacheCarfile(ctx.Context(), info.CID, sources)
			if err != nil {
				log.Errorf("%s pull asset err:%s", node.NodeID, err.Error())
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

func (m *Manager) handleFinished(ctx statemachine.Context, info AssetPullingInfo) error {
	log.Debugf("handle asset finalize: %s", info.CID)
	m.removeAssetTicker(info.Hash.String())

	return nil
}

func (m *Manager) handlePullsFailed(ctx statemachine.Context, info AssetPullingInfo) error {
	m.removeAssetTicker(info.Hash.String())

	if info.RetryCount >= int64(MaxRetryCount) {
		return nil
	}

	log.Debugf("handle pulls failed: %s, retries: %d", info.Hash, info.RetryCount+1)

	if err := failedCoolDown(ctx, info); err != nil {
		return err
	}

	return ctx.Send(AssetRePull{})
}
