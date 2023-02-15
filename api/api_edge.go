package api

import (
	"context"
)

type Edge interface {
	Common
	Device
	// Block
	Download
	Validate
	DataSync
	CarfileOperation
	WaitQuiet(ctx context.Context) error //perm:read
	PingUser(ctx context.Context, userAddrss string) error
}
