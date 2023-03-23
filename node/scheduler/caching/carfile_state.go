package caching

// CarfileState carfile cache state
type CarfileState string

const (
	// UndefinedState Undefined
	UndefinedState CarfileState = ""
	// CacheCarfileSeed cache first carfile to candidate
	CacheCarfileSeed CarfileState = "CacheCarfileSeed"
	// CarfileSeedCaching carfile seed caching
	CarfileSeedCaching CarfileState = "CarfileSeedCaching "
	// FindCandidatesToCache find candidates to cache
	FindCandidatesToCache CarfileState = "FindCandidatesToCache"
	// CandidatesCaching candidates caching
	CandidatesCaching CarfileState = "CandidatesCaching"
	// FindEdgesToCache find edges to cache
	FindEdgesToCache CarfileState = "FindEdgesToCache"
	// EdgesCaching edges caching
	EdgesCaching CarfileState = "EdgesCaching"
	// Finished finished
	Finished CarfileState = "Finished"
	// SeedCacheFailed carfile seed cache failed
	SeedCacheFailed CarfileState = "SeedCacheFailed"
	// CandidatesCacheFailed cache to candidates failed
	CandidatesCacheFailed CarfileState = "CandidatesCacheFailed"
	// EdgesCacheFailed cache to edge failed
	EdgesCacheFailed CarfileState = "CacheEdgesFailed"
	// Removing remove
	Removing CarfileState = "Removing"
)

func (s CarfileState) String() string {
	return string(s)
}

var (
	// FailedStates carfile cache failed states
	FailedStates = []string{
		SeedCacheFailed.String(),
		CandidatesCacheFailed.String(),
		EdgesCacheFailed.String(),
	}

	// CachingStates carfile caching states
	CachingStates = []string{
		CacheCarfileSeed.String(),
		CarfileSeedCaching.String(),
		FindCandidatesToCache.String(),
		CandidatesCaching.String(),
		FindEdgesToCache.String(),
		EdgesCaching.String(),
	}
)
