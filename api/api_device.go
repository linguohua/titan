package api

import (
	"context"

	"github.com/linguohua/titan/api/types"
)

type Device interface {
	NodeInfo(ctx context.Context) (types.NodeInfo, error) //perm:read
	NodeID(ctx context.Context) (string, error)           //perm:read
}
