package api

import "context"

type Edge interface {
	Common
	Device
	Block
	Download
	Validate
	WaitQuiet(ctx context.Context) error //perm:read
}
