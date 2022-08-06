package api

import "context"

type Scheduler interface {
	Common

	EdgeNodeConnect(context.Context, string) error //perm:read

	NotifyNodeCacheData(context.Context, string, string) error //perm:read

	FindNodeWithData(context.Context, string, string) (string, error) //perm:read

	GetDownloadURLWithData(context.Context, string, string) (string, error) //perm:read

	CandidateNodeConnect(context.Context, string) error //perm:read

	CacheResult(context.Context, string, string, bool) error //perm:read

	GetIndexInfo(ctx context.Context, p IndexRequest) (IndexPageRes, error) //perm:read

	ElectionValidators(ctx context.Context) error //perm:read

	SpotCheck(ctx context.Context) error //perm:read
}
