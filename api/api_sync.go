package api

import "context"

type DataSync interface {
	// hash to check block store data consistent
	GetSummaryCheckSum(ctx context.Context) (string, error)   //perm:read
	ScrubBlocks(ctx context.Context, scrub ScrubBlocks) error //perm:write
}
