package storage

// CarfileState
type CarfileState string

var ExistSectorStateList = map[CarfileState]struct{}{}

const (
	UndefinedCarfileState CarfileState = ""
	// StartCache            CarfileState = "StartCache"
	CacheCarfileSeed      CarfileState = "CacheCarfileSeed"
	CarfileSeedCaching    CarfileState = "CarfileSeedCaching "
	CacheToCandidates     CarfileState = "CacheToCandidates"
	CandidatesCaching     CarfileState = "CandidatesCaching"
	CacheToEdges          CarfileState = "CacheToEdges"
	EdgesCaching          CarfileState = "EdgesCaching"
	Finalize              CarfileState = "Finalize"
	CacheSeedFailed       CarfileState = "CacheSeedFailed"
	CacheCandidatesFailed CarfileState = "CacheCandidatesFailed"
	CacheEdgesFailed      CarfileState = "CacheEdgesFailed"
)

func (s CarfileState) String() string {
	return string(s)
}
