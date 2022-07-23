package api

import "context"

type Edge interface {
	Common

	WaitQuiet(ctx context.Context) error //perm:read

	CacheData(ctx context.Context, cid []string) error //perm:write
	StoreStat(ctx context.Context) error               //perm:read
	DeviceID(ctx context.Context) (string, error)      //perm:read
}
