package api

import "context"

type Download interface {
	// set download server upload speed
	SetDownloadSpeed(ctx context.Context, speed int64) error //perm:write
	// get download info
	GetDownloadInfo(ctx context.Context) (DownloadInfo, error) //perm:read
}

type DownloadInfo struct {
	URL   string
	Token string
}
