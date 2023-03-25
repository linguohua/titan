package assets

import (
	"github.com/linguohua/titan/api/types"
	"golang.org/x/xerrors"
)

type mutator interface {
	apply(state *AssetCachingInfo)
}

// globalMutator is an event which can apply in every state
type globalMutator interface {
	// applyGlobal applies the event to the state. If if returns true,
	//  event processing should be interrupted
	applyGlobal(state *AssetCachingInfo) bool
}

// Ignorable Ignorable
type Ignorable interface {
	Ignore()
}

// Global events

// CacheAssetRestart restart
type CacheAssetRestart struct{}

func (evt CacheAssetRestart) applyGlobal(state *AssetCachingInfo) bool {
	state.RetryCount = 0
	return false
}

// CacheAssetFatalError cache asset fatal error
type CacheAssetFatalError struct{ error }

// FormatError Format error
func (evt CacheAssetFatalError) FormatError(xerrors.Printer) (next error) { return evt.error }

func (evt CacheAssetFatalError) applyGlobal(state *AssetCachingInfo) bool {
	log.Errorf("Fatal error on asset %s: %+v", state.CID, evt.error)
	return true
}

// AssetForceState asset force
type AssetForceState struct {
	State AssetState
}

func (evt AssetForceState) applyGlobal(state *AssetCachingInfo) bool {
	state.State = evt.State
	return true
}

// AssetRemove remove
type AssetRemove struct{}

func (evt AssetRemove) applyGlobal(state *AssetCachingInfo) bool {
	*state = AssetCachingInfo{State: Remove}
	return true
}

// InfoUpdate update asset info
type InfoUpdate struct {
	ResultInfo *NodeCacheResultInfo
}

func (evt InfoUpdate) applyGlobal(state *AssetCachingInfo) bool {
	rInfo := evt.ResultInfo
	if rInfo == nil {
		return true
	}

	if state.State == AssetSeedCaching {
		state.Size = rInfo.Size
		state.Blocks = rInfo.BlocksCount
	}

	return true
}

// CacheResult nodes cache result
type CacheResult struct {
	ResultInfo *NodeCacheResultInfo
}

func (evt CacheResult) apply(state *AssetCachingInfo) {
	rInfo := evt.ResultInfo
	if rInfo == nil {
		return
	}

	if state.State == AssetSeedCaching {
		state.Size = rInfo.Size
		state.Blocks = rInfo.BlocksCount
	}

	nodeID := rInfo.NodeID
	if rInfo.Status == int64(types.CacheStatusSucceeded) {
		if rInfo.IsCandidate {
			if !exist(state.CandidateReplicaSucceeds, nodeID) {
				state.CandidateReplicaSucceeds = append(state.CandidateReplicaSucceeds, nodeID)
			}
		} else {
			if !exist(state.EdgeReplicaSucceeds, nodeID) {
				state.EdgeReplicaSucceeds = append(state.EdgeReplicaSucceeds, nodeID)
			}
		}
	} else if rInfo.Status == int64(types.CacheStatusFailed) {
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

// CacheRequestSent request nodes cache asset
type CacheRequestSent struct{}

func (evt CacheRequestSent) apply(state *AssetCachingInfo) {
	state.CandidateReplicaFailures = make([]string, 0)
	state.EdgeReplicaFailures = make([]string, 0)
}

// Normal path

// AssetStartCaches start caches
type AssetStartCaches struct {
	ID                          string
	Hash                        AssetHash
	Replicas                    int64
	ServerID                    string
	CreatedAt                   int64
	Expiration                  int64
	CandidateReplicaCachesCount int
}

func (evt AssetStartCaches) apply(state *AssetCachingInfo) {
	state.CID = evt.ID
	state.Hash = evt.Hash
	state.EdgeReplicas = evt.Replicas
	state.ServerID = evt.ServerID
	state.CreatedAt = evt.CreatedAt
	state.Expiration = evt.Expiration
	state.CandidateReplicas = int64(seedCacheCount + evt.CandidateReplicaCachesCount)
}

// AssetReCache get first asset to candidate
type AssetReCache struct{}

func (evt AssetReCache) apply(state *AssetCachingInfo) {}

// CacheSucceed nodes cache asset completed
type CacheSucceed struct{}

func (evt CacheSucceed) apply(state *AssetCachingInfo) {
	state.RetryCount = 0
}

// CacheFailed nodes cache asset failed
type CacheFailed struct{ error }

// FormatError Format error
func (evt CacheFailed) FormatError(xerrors.Printer) (next error) { return evt.error }

func (evt CacheFailed) apply(state *AssetCachingInfo) {
	state.RetryCount++
}
