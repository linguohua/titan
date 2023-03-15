package api

import "context"

type Candidate interface {
	Common
	Device
	Validate
	DataSync
	CarfileOperation
	WaitQuiet(ctx context.Context) error                                                                                   //perm:read
	GetBlock(ctx context.Context, cid string) ([]byte, error)                                                              //perm:read
	GetBlocksOfCarfile(ctx context.Context, carfileCID string, randomSeed int64, randmonCount int) (map[int]string, error) //perm:read
	ValidateNodes(ctx context.Context, req []ReqValidate) error                                                            //perm:read
}

type ReqValidate struct {
	NodeURL    string
	CarfileCID string
	RandomSeed int64
	// seconds
	Duration int
	RoundID  string
	// node type, for example, edge or candidate
	NodeType int
}

// ValidatedResult node Validate result
type ValidatedResult struct {
	Validator  string
	CarfileCID string
	// verification canceled due to download
	IsCancel  bool
	NodeID    string
	Bandwidth float64
	// millisecond duration
	CostTime  int64
	IsTimeout bool

	// key is random index
	// values is cid
	Cids []string
	// The number of random for validator
	RandomCount int

	RoundID string
}
