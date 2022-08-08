package api

import "context"

type Edge interface {
	Common
	Device
	WaitQuiet(ctx context.Context) error //perm:read

	CacheData(ctx context.Context, req []ReqCacheData) error            //perm:read
	BlockStoreStat(ctx context.Context) error                           //perm:read
	LoadData(ctx context.Context, cid string) ([]byte, error)           //perm:read
	LoadDataByVerifier(ctx context.Context, fid string) ([]byte, error) //perm:read
	CacheFailResult(ctx context.Context) ([]FailResult, error)          //perm:read
}

type ReqCacheData struct {
	ID  string
	Cid string
}

type FailResult struct {
	Cid   string
	Stats bool // cache success or failed
}
