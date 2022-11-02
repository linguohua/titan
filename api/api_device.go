package api

import "context"

type Device interface {
	DeviceInfo(ctx context.Context) (DevicesInfo, error) //perm:read
}
