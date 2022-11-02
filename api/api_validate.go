package api

import "context"

type Validate interface {
	BeValidate(ctx context.Context, reqValidate ReqValidate, candidateTcpSrvAddr string) error //perm:read
}
