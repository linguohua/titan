package api

import "context"

type Candidate interface {
	Edge
	VerifyData(ctx context.Context, req []ReqVerify) error              //perm:read
	SendBlock(ctx context.Context, block []byte, deviceID string) error //perm:read
}

type ReqVerify struct {
	EdgeURL string
	Seed    int64
	FIDs    []string
	// seconds
	Duration int

	RoundID string

	MaxRange int // 废弃
}

type VerifyResult struct {
	Fid string
	Cid string
}

type VerifyResults struct {
	DeviceID  string
	Bandwidth float64
	// microsecond
	CostTime  int
	IsTimeout bool
	Results   []VerifyResult

	RoundID string
}
