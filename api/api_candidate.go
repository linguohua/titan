package api

import "context"

type Candidate interface {
	Edge
	ValidateData(ctx context.Context, req []ReqValidate) error          //perm:read
	SendBlock(ctx context.Context, block []byte, deviceID string) error //perm:read
}

type ReqValidate struct {
	EdgeURL string
	Seed    int64
	FIDs    []string
	// seconds
	Duration int

	RoundID string

	// MaxRange int // 废弃
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
