package storage

// CarfileID is an identifier for a carfile.
type CarfileID string

func (c CarfileID) String() string {
	return string(c)
}

type Log struct {
	Timestamp uint64
	Trace     string // for errors

	Message string

	// additional data (Event info)
	Kind string
}

type CarfileInfo struct {
	State      CarfileState
	CarfileCID CarfileID

	Log []Log
}
