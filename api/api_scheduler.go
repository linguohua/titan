package api

import "context"

type Scheduler interface {
	Common

	EdgeNodeConnect(context.Context, string) error //perm:write

	CacheData(context.Context, []string, []string) error //perm:write

	LoadData(context.Context, string, string) ([]byte, error) //perm:read
}
