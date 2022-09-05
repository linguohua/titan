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
	EdgeURL string
	Seed    int64
	FIDs    []string
	// seconds
	Duration int

	RoundID string
}

type ValidateResult struct {
	Fid string
	Cid string
}

type ValidateResults struct {
	DeviceID  string
	Bandwidth float64
	// microsecond
	CostTime  int
	IsTimeout bool
	Results   []ValidateResult

	RoundID string
}
