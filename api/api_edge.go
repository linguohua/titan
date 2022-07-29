package api

import "context"

type Edge interface {
	Common

	WaitQuiet(ctx context.Context) error //perm:read

	CacheData(ctx context.Context, cid []string) error                  //perm:read
	BlockStoreStat(ctx context.Context) error                           //perm:read
	DeviceInfo(ctx context.Context) (DeviceInfo, error)                 //perm:read
	LoadData(ctx context.Context, cid string) ([]byte, error)           //perm:read
	LoadDataByVerifier(ctx context.Context, fid string) ([]byte, error) //perm:read
}

type DeviceInfo struct {
	DeviceID string
	PublicIP string
}
