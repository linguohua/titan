package storage

type CarfileState string

var ExistSectorStateList = map[CarfileState]struct{}{}

const (
	UndefinedCarfileState  CarfileState = ""
	StartCache             CarfileState = "StartCache"
	GetSeed                CarfileState = "GetSeed"
	GetSeedCompleted       CarfileState = "GetSeedCompleted"
	CandidateCaching       CarfileState = "CandidateCaching"
	CandidateCompleted     CarfileState = "CandidateCompleted"
	EdgeCaching            CarfileState = "EdgeCaching"
	EdgeCompleted          CarfileState = "EdgeCompleted"
	Finalize               CarfileState = "Finalize"
	GetSeedFailed          CarfileState = "GetSeedFailed"
	CandidateCachingFailed CarfileState = "CandidateCachingFailed"
	EdgeCachingFailed      CarfileState = "EdgeCachingFailed"
)
