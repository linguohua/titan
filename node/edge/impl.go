package edge

import (
	"context"

	"titan/api"

	"titan/node/common"

	"github.com/ipfs/go-datastore"
)

var deviceID = "15165486494"

func NewLocalEdgeNode(ds datastore.Batching, scheduler api.Scheduler) api.Edge {
	return EdgeAPI{ds: ds, scheduler: scheduler}
}

type EdgeAPI struct {
	common.CommonAPI
	ds        datastore.Batching
	scheduler api.Scheduler
}

func (edge EdgeAPI) WaitQuiet(ctx context.Context) error {
	return nil
}

func (edge EdgeAPI) CacheData(ctx context.Context, cid []string) error {
	return nil
}

func (edge EdgeAPI) StoreStat(ctx context.Context) error {
	return nil
}

func (edge EdgeAPI) DeviceID(ctx context.Context) (string, error) {
	return deviceID, nil
}
