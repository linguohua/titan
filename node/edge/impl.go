package edge

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/api/client"
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

	delayReq := filterAvailableReq(edge, apiReq2DelayReq(req))
	if len(req) == 0 {
		log.Infof("CacheData, len(req) == 0 not need to handle")
		return nil
	}

	reqDatas = append(reqDatas, delayReq...)

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

func (edge EdgeAPI) DoVerify(ctx context.Context, reqVerify api.ReqVerify, candidateURL string) error {
	log.Info("DoVerify")

	if edge.blockStore == nil {
		log.Errorf("DoVerify,edge.blockStore == nil ")
		return fmt.Errorf("edge.blockStore == nil")
	}

	candidateAPI, closer, err := client.NewCandicate(ctx, candidateURL, nil)
	if err != nil {
		log.Errorf("DoVerify, NewCandicate err:%v", err)
		return err
	}
	defer closer()

	r := rand.New(rand.NewSource(reqVerify.Seed))
	t := time.NewTimer(time.Duration(reqVerify.Duration) * time.Second)

	for {
		select {
		case <-t.C:
			return nil
		default:
		}

		fid := r.Intn(reqVerify.MaxRange)
		fidStr := fmt.Sprintf("%d", fid)
		err = sendBlock(ctx, edge, candidateAPI, fidStr)
		if err != nil {
			return err
		}
	}
}

func sendBlock(ctx context.Context, edge EdgeAPI, candidateAPI api.Candidate, fid string) error {
	cid, err := getCID(edge, fid)
	if err != nil {
		log.Errorf("sendBlock, getCID err:%v", err)
		return err
	}

	block, err := edge.blockStore.Get(cid)
	if err != nil {
		log.Errorf("sendBlock, get block err:%v", err)
		return err
	}

	return candidateAPI.SendBlock(ctx, block, edge.DeviceID)
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

func (edge EdgeAPI) DeleteData(ctx context.Context, cids []string) error {
	log.Info("DeleteData")

	if edge.blockStore == nil {
		log.Errorf("DeleteData, blockStore not setting")
		return fmt.Errorf("edge.blockStore == nil")
	}

	for _, id := range cids {
		err := edge.blockStore.Delete(id)
		if err != nil {
			log.Errorf("DeleteData, block %s not exist", id)
		}
	}
	return nil
}
