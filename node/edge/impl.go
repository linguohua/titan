package edge

import (
	"context"
	"titan-ultra-network/api"

	"titan-ultra-network/node/common"

	"github.com/ipfs/go-datastore"
)

func NewLocalEdgeNode(ds datastore.Batching, scheduler api.Scheduler) api.Edge {
	return EdgeAPI{}
}

type EdgeAPI struct {
	common.CommonAPI
}

func (edge EdgeAPI) WaitQuiet(ctx context.Context) error {
	return nil
}

func (edge EdgeAPI) Save(ctx context.Context) error {
	return nil
}
