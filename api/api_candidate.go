package api

import "context"

type Candidate interface {
	Edge
	VerifyData(ctx context.Context, req []ReqVarify) ([]VarifyResult, error) //perm:read
}

type ReqVarify struct {
	Fid string
	URL string
}

type VarifyResult struct {
	Fid       string
	Cid       string
	DeviceID  string
	Bandwidth float64
	// microsecond
	CostTime  int
	IsTimeout bool
}
