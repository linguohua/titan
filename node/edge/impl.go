package edge

import (
	"context"
	"fmt"

	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/build"
	"github.com/linguohua/titan/lib/p2p"
	"github.com/linguohua/titan/stores"
	"golang.org/x/time/rate"

	"github.com/linguohua/titan/node/common"

	"github.com/ipfs/go-datastore"
	exchange "github.com/ipfs/go-ipfs-exchange-interface"
	logging "github.com/ipfs/go-log/v2"
)

var log = logging.Logger("main")

func NewLocalEdgeNode(ctx context.Context, ds datastore.Batching, scheduler api.Scheduler, blockStore stores.BlockStore, deviceID, publicIP string) api.Edge {
	addrs, err := build.BuiltinBootstrap()
	if err != nil {
		log.Fatal(err)
	}

	exchange, err := p2p.Bootstrap(ctx, addrs)
	if err != nil {
		log.Fatal(err)
	}
	return EdgeAPI{
		ds:         ds,
		scheduler:  scheduler,
		blockStore: blockStore,
		deviceID:   deviceID,
		limiter:    rate.NewLimiter(rate.Limit(0), 0),
		publicIP:   publicIP,
		exchange:   exchange}
}

type EdgeAPI struct {
	common.CommonAPI
	ds         datastore.Batching
	scheduler  api.Scheduler
	blockStore stores.BlockStore
	deviceID   string
	limiter    *rate.Limiter
	publicIP   string
	exchange   exchange.Interface
}

func (edge EdgeAPI) WaitQuiet(ctx context.Context) error {
	return nil
}

func (edge EdgeAPI) CacheData(ctx context.Context, req []api.ReqCacheData) error {
	if edge.blockStore == nil {
		return fmt.Errorf("CacheData, blockStore == nil ")
	}

	go loadBlocks(edge, req)
	return nil
}

func (edge EdgeAPI) BlockStoreStat(ctx context.Context) error {
	return nil
}

func (edge EdgeAPI) DeviceInfo(ctx context.Context) (api.DevicesInfo, error) {
	info := api.DevicesInfo{DeviceId: edge.deviceID, ExternalIp: edge.publicIP}
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
