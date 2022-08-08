package api

import "context"

type Candidate interface {
	Common

	WaitQuiet(ctx context.Context) error                                     //perm:read
	VerifyData(ctx context.Context, req []ReqVarify) ([]VarifyResult, error) //perm:read
}

type ReqVarify struct {
	Fid string
	URL string
}

type VarifyResult struct {
	Fid       string
	Cid       string
	Bandwidth float64
	// microsecond
	CostTime  int
	IsTimeout bool
}
