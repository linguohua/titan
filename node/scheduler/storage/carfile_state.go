package storage

type CarfileState string

const (
	UndefinedCarfileState CarfileState = ""
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
	Remove                CarfileState = "Remove"
)

func (s CarfileState) String() string {
	return string(s)
}
