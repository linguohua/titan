package assets

import (
	"github.com/linguohua/titan/api/types"
	"golang.org/x/xerrors"
)

type mutator interface {
	apply(state *AssetPullingInfo)
}

// globalMutator is an event which can apply in every state
type globalMutator interface {
	// applyGlobal applies the event to the state. If if returns true,
	//  event processing should be interrupted
	applyGlobal(state *AssetPullingInfo) bool
}

// Ignorable Ignorable
type Ignorable interface {
	Ignore()
}

// Global events

// PullAssetRestart restart
type PullAssetRestart struct{}

func (evt PullAssetRestart) applyGlobal(state *AssetPullingInfo) bool {
	state.RetryCount = 0
	return false
}

// PullAssetFatalError pull asset fatal error
type PullAssetFatalError struct{ error }

// FormatError Format error
func (evt PullAssetFatalError) FormatError(xerrors.Printer) (next error) { return evt.error }

func (evt PullAssetFatalError) applyGlobal(state *AssetPullingInfo) bool {
	log.Errorf("Fatal error on asset %s: %+v", state.CID, evt.error)
	return true
}

// AssetForceState asset force
type AssetForceState struct {
	State AssetState
}

func (evt AssetForceState) applyGlobal(state *AssetPullingInfo) bool {
	state.State = evt.State
	return true
}

// AssetRemove remove
type AssetRemove struct{}

func (evt AssetRemove) applyGlobal(state *AssetPullingInfo) bool {
	*state = AssetPullingInfo{State: Remove}
	return true
}

// InfoUpdate update asset info
type InfoUpdate struct {
	ResultInfo *NodePulledResult
}

func (evt InfoUpdate) applyGlobal(state *AssetPullingInfo) bool {
	rInfo := evt.ResultInfo
	if rInfo == nil {
		return true
	}

	if state.State == AssetSeedPulling {
		state.Size = rInfo.Size
		state.Blocks = rInfo.BlocksCount
	}

	return true
}

// PulledResult nodes pull result
type PulledResult struct {
	ResultInfo *NodePulledResult
}

func (evt PulledResult) apply(state *AssetPullingInfo) {
	rInfo := evt.ResultInfo
	if rInfo == nil {
		return
	}

	if state.State == AssetSeedPulling {
		state.Size = rInfo.Size
		state.Blocks = rInfo.BlocksCount
	}

	nodeID := rInfo.NodeID
	if rInfo.Status == int64(types.ReplicaStatusSucceeded) {
		if rInfo.IsCandidate {
			if !exist(state.CandidateReplicaSucceeds, nodeID) {
				state.CandidateReplicaSucceeds = append(state.CandidateReplicaSucceeds, nodeID)
			}
		} else {
			if !exist(state.EdgeReplicaSucceeds, nodeID) {
				state.EdgeReplicaSucceeds = append(state.EdgeReplicaSucceeds, nodeID)
			}
		}
	} else if rInfo.Status == int64(types.ReplicaStatusFailed) {
		if rInfo.IsCandidate {
			if !exist(state.CandidateReplicaFailures, nodeID) {
				state.CandidateReplicaFailures = append(state.CandidateReplicaFailures, nodeID)
			}
		} else {
			if !exist(state.EdgeReplicaFailures, nodeID) {
				state.EdgeReplicaFailures = append(state.EdgeReplicaFailures, nodeID)
			}
		}
	}
}

func exist(list []string, c string) bool {
	for _, str := range list {
		if str == c {
			return true
		}
	}

	return false
}

// PullRequestSent request nodes pull asset
type PullRequestSent struct{}

func (evt PullRequestSent) apply(state *AssetPullingInfo) {
	state.CandidateReplicaFailures = make([]string, 0)
	state.EdgeReplicaFailures = make([]string, 0)
}

// Normal path

// AssetStartPulls start pulls
type AssetStartPulls struct {
	ID                string
	Hash              AssetHash
	Replicas          int64
	ServerID          string
	CreatedAt         int64
	Expiration        int64
	CandidateReplicas int // Number of candidate node replicas
}

func (evt AssetStartPulls) apply(state *AssetPullingInfo) {
	state.CID = evt.ID
	state.Hash = evt.Hash
	state.EdgeReplicas = evt.Replicas
	state.ServerID = evt.ServerID
	state.CreatedAt = evt.CreatedAt
	state.Expiration = evt.Expiration
	state.CandidateReplicas = int64(seedReplicaCount + evt.CandidateReplicas)
}

// AssetRePull get first asset to candidate
type AssetRePull struct{}

func (evt AssetRePull) apply(state *AssetPullingInfo) {}

// PullSucceed nodes pull asset completed
type PullSucceed struct{}

func (evt PullSucceed) apply(state *AssetPullingInfo) {
	state.RetryCount = 0
}

// PullFailed nodes pull asset failed
type PullFailed struct{ error }

// FormatError Format error
func (evt PullFailed) FormatError(xerrors.Printer) (next error) { return evt.error }

func (evt PullFailed) apply(state *AssetPullingInfo) {
	state.RetryCount++
}
