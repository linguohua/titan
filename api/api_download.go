package api

import "context"

type Download interface {
	// set download server upload speed
	SetDownloadSpeed(ctx context.Context, speed int64) error //perm:write
}

type DownloadInfo struct {
	URL      string
	Sign     []byte
	SN       int64
	SignTime int64
	TimeOut  int
}
