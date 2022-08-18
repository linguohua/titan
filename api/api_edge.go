package api

import "context"

type Edge interface {
	Common
	Device
	WaitQuiet(ctx context.Context) error //perm:read

	CacheData(ctx context.Context, req []ReqCacheData) error                      //perm:read
	BlockStoreStat(ctx context.Context) error                                     //perm:read
	LoadData(ctx context.Context, cid string) ([]byte, error)                     //perm:read
	DoVerify(ctx context.Context, reqVerify ReqVerify, candidateURL string) error //perm:read
	DeleteData(ctx context.Context, cids []string) error                          //perm:read
}

type ReqCacheData struct {
	ID  string
	Cid string
}
