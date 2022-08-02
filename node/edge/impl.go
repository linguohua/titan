package edge

import (
	"context"
	"fmt"

	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/stores"
	"golang.org/x/time/rate"

	"github.com/linguohua/titan/node/common"

	"github.com/ipfs/go-datastore"
	logging "github.com/ipfs/go-log/v2"
)

var log = logging.Logger("main")

func NewLocalEdgeNode(ds datastore.Batching, scheduler api.Scheduler, blockStore stores.BlockStore, deviceID, publicIP string) api.Edge {
	return EdgeAPI{ds: ds, scheduler: scheduler, blockStore: blockStore, deviceID: deviceID, publicIP: publicIP, limiter: rate.NewLimiter(rate.Limit(0), 0)}
}

type EdgeAPI struct {
	common.CommonAPI
	ds         datastore.Batching
	scheduler  api.Scheduler
	blockStore stores.BlockStore
	deviceID   string
	limiter    *rate.Limiter
	publicIP   string
}

func (edge EdgeAPI) WaitQuiet(ctx context.Context) error {
	return nil
}

func (edge EdgeAPI) CacheData(ctx context.Context, cids []string) error {
	if edge.blockStore == nil {
		return fmt.Errorf("CacheData, blockStore == nil ")
	}

	go loadBlocks(edge, cids)
	return nil
}

func (edge EdgeAPI) BlockStoreStat(ctx context.Context) error {
	return nil
}

func (edge EdgeAPI) DeviceInfo(ctx context.Context) (api.DeviceInfo, error) {
	info := api.DeviceInfo{DeviceID: edge.deviceID, PublicIP: edge.publicIP}
	return info, nil
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
