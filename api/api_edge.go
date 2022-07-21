package api

import "context"

type Edge interface {
	Common

	WaitQuiet(ctx context.Context) error
}
