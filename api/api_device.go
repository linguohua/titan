package api

import "context"

type Device interface {
	DeviceInfo(ctx context.Context) (DeviceInfo, error) //perm:read
	NodeID(ctx context.Context) (string, error)         //perm:read
}
