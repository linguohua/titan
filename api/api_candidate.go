package api

import "context"

type Candidate interface {
	Common
	Device
	Validate
	DataSync
	CarfileOperation
	WaitQuiet(ctx context.Context) error                                                                                  //perm:read                                                        //perm:read
	GetBlocksOfCarfile(ctx context.Context, carfileCID string, randomSeed int64, randomCount int) (map[int]string, error) //perm:read
}

// ValidateResult node Validate result
type ValidateResult struct {
	Validator string
	CID       string
	// verification canceled due to download
	IsCancel  bool
	NodeID    string
	Bandwidth float64
	// seconds duration
	CostTime  int64
	IsTimeout bool

	// key is random index
	// values is cid
	Cids []string
	// The number of random for validator
	RandomCount int

	RoundID string
}
