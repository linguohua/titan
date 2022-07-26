package api

import "context"

type Validator interface {
	Common

	WaitQuiet(ctx context.Context) error                                    //perm:read
	VerifyData(ctx context.Context, fid string, url string) (string, error) //perm:read
}
