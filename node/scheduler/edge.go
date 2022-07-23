package scheduler

import (
	"titan-ultra-network/api"

	"github.com/filecoin-project/go-jsonrpc"
)

// EdgeNode Edge 节点
type EdgeNode struct {
	edgeAPI api.Edge
	closer  jsonrpc.ClientCloser

	deviceID string
	addr     string
	userID   string
}
