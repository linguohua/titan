package edge

import (
	"context"
	"fmt"

	"titan/api"
	"titan/stores"

	"titan/node/common"

	"github.com/ipfs/go-datastore"
	logging "github.com/ipfs/go-log/v2"
)

var log = logging.Logger("main")
var deviceID = "123456789000000000"

func NewLocalEdgeNode(ds datastore.Batching, scheduler api.Scheduler, blockStore stores.BlockStore) api.Edge {
	return EdgeAPI{ds: ds, scheduler: scheduler, blockStore: blockStore}
}

type EdgeAPI struct {
	common.CommonAPI
	ds         datastore.Batching
	scheduler  api.Scheduler
	blockStore stores.BlockStore
}

func (edge EdgeAPI) WaitQuiet(ctx context.Context) error {
	return nil
}

func (edge EdgeAPI) CacheData(ctx context.Context, cids []string) error {
	if edge.blockStore == nil {
		return fmt.Errorf("CacheData, blockStore == nil ")
	}

	for _, cid := range cids {
		data, err := loadBlock(cid)
		if err != nil {
			log.Infof("CacheData, loadBlock error:", err)
			return err
		}

		edge.blockStore.Put(data, cid)
	}
	return nil
}

func (edge EdgeAPI) StoreStat(ctx context.Context) error {
	return nil
}

func (edge EdgeAPI) DeviceID(ctx context.Context) (string, error) {
	return deviceID, nil
}
