package storage

type CarfileState string

var ExistSectorStateList = map[CarfileState]struct{}{}

const (
	UndefinedCarfileState CarfileState = ""
	// StartCache            CarfileState = "StartCache"
	GetSeed               CarfileState = "GetSeed"
	GetSeedCaching        CarfileState = "GetSeedCaching "
	StartCandidatesCache  CarfileState = "StartCandidatesCache"
	CandidatesCaching     CarfileState = "CandidatesCaching"
	StartEdgesCache       CarfileState = "StartEdgesCache"
	EdgesCaching          CarfileState = "EdgesCaching"
	Finalize              CarfileState = "Finalize"
	GetSeedFailed         CarfileState = "GetSeedFailed"
	CandidatesCacheFailed CarfileState = "CandidatesCacheFailed"
	EdgesCacheFailed      CarfileState = "EdgesCacheFailed"
)
