package api

import "context"

type Edge interface {
	Common
	Device
	WaitQuiet(ctx context.Context) error //perm:read
	// call by scheduler
	CacheData(ctx context.Context, req ReqCacheData) error //perm:read
	// call by scheduler
	DeleteData(ctx context.Context, cids []string) (DelResult, error)             //perm:read
	LoadData(ctx context.Context, cid string) ([]byte, error)                     //perm:read
	BlockStoreStat(ctx context.Context) error                                     //perm:read
	DoVerify(ctx context.Context, reqVerify ReqVerify, candidateURL string) error //perm:read
	// call by edge or candidate
	DeleteBlocks(ctx context.Context, cid []string) (DelResult, error) //perm:read
}

type ReqCacheData struct {
	Cids         []string
	CandidateURL string
}

type DelFailed struct {
	Cid    string
	ErrMsg string
}
type DelResult struct {
	List []DelFailed
}
