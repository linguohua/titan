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

// var deviceID = "123456789000000000"

func NewLocalEdgeNode(ds datastore.Batching, scheduler api.Scheduler, blockStore stores.BlockStore, deviceID string) api.Edge {
	return EdgeAPI{ds: ds, scheduler: scheduler, blockStore: blockStore, deviceID: deviceID}
}

type EdgeAPI struct {
	common.CommonAPI
	ds         datastore.Batching
	scheduler  api.Scheduler
	blockStore stores.BlockStore
	deviceID   string
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
			log.Errorf("CacheData, loadBlock error:", err)
			return err
		}

		err = edge.blockStore.Put(data, cid)
		if err != nil {
			log.Errorf("CacheData, put error:", err)
			return err
		}
	}
	return nil
}

func (edge EdgeAPI) BlockStoreStat(ctx context.Context) error {
	return nil
}

func (edge EdgeAPI) DeviceID(ctx context.Context) (string, error) {
	return edge.deviceID, nil
}

func (edge EdgeAPI) LoadData(ctx context.Context, cid string) ([]byte, error) {
	if edge.blockStore == nil {
		log.Errorf("CacheData, blockStore not setting")
		return nil, nil
	}

	return edge.blockStore.Get(cid)
}

func (edge EdgeAPI) LoadDataByVerifier(ctx context.Context, fileID string) ([]byte, error) {
	if edge.blockStore == nil {
		log.Errorf("CacheData, blockStore not setting")
		return nil, nil
	}

	cid := getCID(fileID)
	return edge.blockStore.Get(cid)
}

func getCID(fid string) string {
	// TODO: 存储fid到cid的映射，然后从映射里面读取
	return fid
}
