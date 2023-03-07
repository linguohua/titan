package storage

type CarfileState string

var ExistSectorStateList = map[CarfileState]struct{}{}

const (
	UndefinedCarfileState CarfileState = ""
	PreparingState        CarfileState = "Preparing"
	CarfileStartState     CarfileState = "CarfileStart"
	CarfileFinishedState  CarfileState = "CarfileFinished"
)
