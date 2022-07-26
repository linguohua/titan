package api

import "context"

type Scheduler interface {
	Common

	EdgeNodeConnect(context.Context, string) error //perm:read

	CacheData(context.Context, []string, []string) error //perm:read

	LoadData(context.Context, string, string) ([]byte, error) //perm:read
}
