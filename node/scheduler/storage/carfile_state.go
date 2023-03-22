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
	Finished              CarfileState = "Finished"
	CacheSeedFailed       CarfileState = "CacheSeedFailed"
	CacheCandidatesFailed CarfileState = "CacheCandidatesFailed"
	CacheEdgesFailed      CarfileState = "CacheEdgesFailed"
	Removing              CarfileState = "Removing"
)

func (s CarfileState) String() string {
	return string(s)
}
