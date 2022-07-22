package scheduler

import (
	"titan-ultra-network/api"

	"github.com/filecoin-project/go-jsonrpc"
)

// Edge Edge类型
type Edge struct {
	edgeAPI api.Edge
	closer  jsonrpc.ClientCloser

	addr string
}
