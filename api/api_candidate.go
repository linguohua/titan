package api

import "context"

type Candidate interface {
	Common
	Device
	Base
	Validate
	ValidateBlocks(ctx context.Context, req []ReqValidate) error //perm:read
}

type ReqValidate struct {
	NodeURL string
	Seed    int64
	// seconds
	Duration int
	RoundID  string
	// node type, for example edge or candidate
	Type string
}

// type ValidateResult struct {
// 	Cid string
// }

type ValidateResults struct {
	DeviceID  string
	Bandwidth float64
	// microsecond
	CostTime  int
	IsTimeout bool
	Cids      []string

	RoundID string
}
