package api

import "context"

type Edge interface {
	Common

	WaitQuiet(ctx context.Context) error //perm:read

	CacheData(ctx context.Context, req []ReqCacheData) error            //perm:read
	BlockStoreStat(ctx context.Context) error                           //perm:read
	DeviceInfo(ctx context.Context) (DevicesInfo, error)                //perm:read
	LoadData(ctx context.Context, cid string) ([]byte, error)           //perm:read
	LoadDataByVerifier(ctx context.Context, fid string) ([]byte, error) //perm:read
	CacheFailResult(ctx context.Context) ([]FailResult, error)
}

type ReqCacheData struct {
	ID  string
	Cid string
}

type FailResult struct {
	Cid   string
	Stats bool // cache success or failed
}
