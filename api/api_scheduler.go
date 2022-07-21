package api

import "context"

type Scheduler interface {
	Common

	EdgeNodeConnect(context.Context, string) error
}
