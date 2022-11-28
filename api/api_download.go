package api

import "context"

type Download interface {
	// set download server upload speed
	SetDownloadSpeed(ctx context.Context, speed int64) error //perm:write
}
