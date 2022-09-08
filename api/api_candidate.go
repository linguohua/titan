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
	// node type, for example edge or candidate
	NodeType int
}

type ValidateResults struct {
	DeviceID  string
	Bandwidth float64
	// microsecond
	CostTime  int
	IsTimeout bool
	Cids      []string

	RoundID string
}
