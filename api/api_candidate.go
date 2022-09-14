package api

import "context"

type Candidate interface {
	Common
	Device
	Block
	Download
	Validate
	WaitQuiet(ctx context.Context) error                         //perm:read
	ValidateBlocks(ctx context.Context, req []ReqValidate) error //perm:read
}

type ReqValidate struct {
	NodeURL string
	Seed    int64
	// seconds
	Duration int
	RoundID  string
	// node type, for example, edge or candidate
	NodeType int
	// current max fid
	MaxFid int
}

type ValidateResults struct {
	DeviceID  string
	Bandwidth float64
	// microsecond
	CostTime  int
	IsTimeout bool
	// key is random index
	// values is cid
	Cids map[int]string
	// The number of random for validate
	// if radmon fid all exist, RandomCount == len(Cids)
	RandomCount int

	RoundID string
}
