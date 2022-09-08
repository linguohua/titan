package api

import "context"

type Download interface {
	// set download server upload speed
	SetDownloadSpeed(ctx context.Context, speed int64) error //perm:read
	// generate token for user to download block
	GenerateDownloadToken(ctx context.Context) (string, error) //perm:read
}
