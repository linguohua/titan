package api

import (
	"context"
)

type Edge interface {
	Common
	Device
	Block
	Download
	Validate
	DataSync
	WaitQuiet(ctx context.Context) error //perm:read
}
