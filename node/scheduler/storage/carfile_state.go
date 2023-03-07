package storage

type CarfileState string

var ExistSectorStateList = map[CarfileState]struct{}{}

const (
	UndefinedCarfileState  CarfileState = ""
	GetCarfile             CarfileState = "GetCarfile"
	GetCarfileCompleted    CarfileState = "GetCarfileCompleted"
	CandidateCaching       CarfileState = "CandidateCaching"
	CandidateCompleted     CarfileState = "CandidateCompleted"
	EdgeCaching            CarfileState = "EdgeCaching"
	EdgeCompleted          CarfileState = "EdgeCompleted"
	Finalize               CarfileState = "Finalize"
	GetCarfileFailed       CarfileState = "GetCarfileFailed"
	CandidateCachingFailed CarfileState = "CandidateCachingFailed"
	EdgeCachingFailed      CarfileState = "EdgeCachingFailed"
)
