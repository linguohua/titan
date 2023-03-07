package api

import "context"

type Device interface {
	NodeInfo(ctx context.Context) (NodeInfo, error) //perm:read
	NodeID(ctx context.Context) (string, error)     //perm:read
}
