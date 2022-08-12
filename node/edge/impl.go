package edge

import (
	"context"
	"fmt"

	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/build"
	"github.com/linguohua/titan/lib/p2p"
	"github.com/linguohua/titan/stores"
	"golang.org/x/time/rate"

	"github.com/ipfs/go-datastore"
	exchange "github.com/ipfs/go-ipfs-exchange-interface"
	logging "github.com/ipfs/go-log/v2"
	"github.com/linguohua/titan/node/common"
	"github.com/linguohua/titan/node/device"
)

var log = logging.Logger("edge")

func NewLocalEdgeNode(ctx context.Context, ds datastore.Batching, scheduler api.Scheduler, blockStore stores.BlockStore, device device.DeviceAPI) api.Edge {
	addrs, err := build.BuiltinBootstrap()
	if err != nil {
		log.Fatal(err)
	}

	exchange, err := p2p.Bootstrap(ctx, addrs)
	if err != nil {
		log.Fatal(err)
	}
	edge := EdgeAPI{
		ds:         ds,
		scheduler:  scheduler,
		blockStore: blockStore,
		limiter:    rate.NewLimiter(rate.Inf, 0),
		exchange:   exchange,
		DeviceAPI:  device,
	}

	go startBlockLoader(ctx, edge)

	return edge
}

type EdgeAPI struct {
	common.CommonAPI
	device.DeviceAPI
	ds         datastore.Batching
	blockStore stores.BlockStore
	scheduler  api.Scheduler
	limiter    *rate.Limiter
	exchange   exchange.Interface
}

func (edge EdgeAPI) WaitQuiet(ctx context.Context) error {
	log.Info("WaitQuiet")
	return nil
}

func (edge EdgeAPI) CacheData(ctx context.Context, req []api.ReqCacheData) error {
	log.Infof("CacheData, req len:%d", len(req))

	if edge.blockStore == nil {
		return fmt.Errorf("CacheData, blockStore == nil ")
	}

	req = filterAvailableReq(edge, req)
	reqDatas = append(reqDatas, req...)

	// go loadBlocksOneByOne(edge, req)
	return nil
}

func (edge EdgeAPI) BlockStoreStat(ctx context.Context) error {
	log.Info("BlockStoreStat")

	return nil
}

func (edge EdgeAPI) LoadData(ctx context.Context, cid string) ([]byte, error) {
	log.Info("LoadData")

	if edge.blockStore == nil {
		log.Errorf("LoadData, blockStore not setting")
		return nil, nil
	}

	return edge.blockStore.Get(cid)
}

func (edge EdgeAPI) LoadDataByVerifier(ctx context.Context, fileID string) ([]byte, error) {
	log.Info("LoadDataByVerifier")

	if edge.blockStore == nil {
		log.Errorf("LoadDataByVerifier, blockStore not setting")
		return nil, nil
	}

	cid, err := getCID(edge, fileID)
	if err != nil {
		log.Errorf("LoadDataByVerifier, getCID err:%v", err)
		return nil, err
	}
	return edge.blockStore.Get(cid)
}

func getCID(edge EdgeAPI, fid string) (string, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	value, err := edge.ds.Get(ctx, datastore.NewKey(fid))
	if err != nil {
		log.Errorf("Get cid from store error:%v, fid:%s", err, fid)
		return "", err
	}

	return string(value), nil
}
