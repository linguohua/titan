package api

import "context"

type Candidate interface {
	Common
	Device
	// Block
	Download
	Validate
	DataSync
	CarfileOperation
	WaitQuiet(ctx context.Context) error                         //perm:read
	ValidateBlocks(ctx context.Context, req []ReqValidate) error //perm:read
	// load block
	LoadBlock(ctx context.Context, cid string) ([]byte, error) //perm:read
}

type ReqValidate struct {
	NodeURL string
	Seed    int64
	// seconds
	Duration int
	RoundID  int64
	// node type, for example, edge or candidate
	NodeType int
	// current max fid
	MaxFid int
}

type ValidateResults struct {
	Validator string
	// verification canceled due to download
	IsCancel  bool
	DeviceID  string
	Bandwidth float64
	// millisecond duration
	CostTime  int64
	IsTimeout bool

	// key is random index
	// values is cid
	Cids map[int]string
	// The number of random for validate
	// if random fid all exist, RandomCount == len(Cids)
	RandomCount int

	RoundID int64
}
