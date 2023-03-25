package assets

// AssetState asset cache state
type AssetState string

const (
	// UndefinedState Undefined
	UndefinedState AssetState = ""
	// CacheAssetSeed cache first asset to candidate
	CacheAssetSeed AssetState = "CacheAssetSeed"
	// AssetSeedCaching asset seed caching
	AssetSeedCaching AssetState = "AssetSeedCaching "
	// FindCandidatesToCache find candidates to cache
	FindCandidatesToCache AssetState = "FindCandidatesToCache"
	// CandidatesCaching candidates caching
	CandidatesCaching AssetState = "CandidatesCaching"
	// FindEdgesToCache find edges to cache
	FindEdgesToCache AssetState = "FindEdgesToCache"
	// EdgesCaching edges caching
	EdgesCaching AssetState = "EdgesCaching"
	// Finished finished
	Finished AssetState = "Finished"
	// SeedCacheFailed asset seed cache failed
	SeedCacheFailed AssetState = "SeedCacheFailed"
	// CandidatesCacheFailed cache to candidates failed
	CandidatesCacheFailed AssetState = "CandidatesCacheFailed"
	// EdgesCacheFailed cache to edge failed
	EdgesCacheFailed AssetState = "CacheEdgesFailed"
	// Remove remove
	Remove AssetState = "Remove"
)

func (s AssetState) String() string {
	return string(s)
}

var (
	// FailedStates asset cache failed states
	FailedStates = []string{
		SeedCacheFailed.String(),
		CandidatesCacheFailed.String(),
		EdgesCacheFailed.String(),
	}

	// CachingStates asset caching states
	CachingStates = []string{
		CacheAssetSeed.String(),
		AssetSeedCaching.String(),
		FindCandidatesToCache.String(),
		CandidatesCaching.String(),
		FindEdgesToCache.String(),
		EdgesCaching.String(),
	}
)
