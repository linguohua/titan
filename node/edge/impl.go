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

func NewLocalEdgeNode(ctx context.Context, ds datastore.Batching, scheduler api.Scheduler, blockStore stores.BlockStore, device device.DeviceAPI, isCandidate bool) api.Edge {
	addrs, err := build.BuiltinBootstrap()
	if err != nil {
		log.Fatal(err)
	}

	exchange, err := p2p.Bootstrap(ctx, addrs)
	if err != nil {
		log.Fatal(err)
	}
	edge := EdgeAPI{
		ds:          ds,
		scheduler:   scheduler,
		blockStore:  blockStore,
		limiter:     rate.NewLimiter(rate.Inf, 0),
		exchange:    exchange,
		DeviceAPI:   device,
		isCandidate: isCandidate,
	}

	if isCandidate {
		go startLoadBlockFromIPFS(ctx, edge)
	} else {
		go startLoadBlockFromCandidate(ctx, edge)
	}

	return edge
}

type EdgeAPI struct {
	common.CommonAPI
	device.DeviceAPI
	ds          datastore.Batching
	blockStore  stores.BlockStore
	scheduler   api.Scheduler
	limiter     *rate.Limiter
	exchange    exchange.Interface
	isCandidate bool
}

func (edge EdgeAPI) WaitQuiet(ctx context.Context) error {
	log.Info("WaitQuiet")
	return nil
}

func (edge EdgeAPI) CacheData(ctx context.Context, req api.ReqCacheData) error {
	log.Infof("CacheData, req len:%d", len(req.Cids))
	if edge.blockStore == nil {
		return fmt.Errorf("CacheData, blockStore == nil ")
	}

	if !edge.isCandidate {
		return addReq2Queue(edge, req)
	}

	delayReq := filterAvailableReq(edge, apiReq2DelayReq(req))
	if len(delayReq) == 0 {
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

	if reqVerify.MaxRange <= 0 {
		log.Errorf("reqVerify.MaxRange <= 0")
		return fmt.Errorf("reqVerify.MaxRange <= 0")
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
		fid := reqVerify.MaxRange
		if reqVerify.MaxRange > 1 {
			fid = r.Intn(reqVerify.MaxRange-1) + 1
		}
		fidStr := fmt.Sprintf("%d", fid)
		err = sendBlock(ctx, edge, candidateAPI, fidStr)
		if err != nil {
			return err
		}
	}
}

func sendBlock(ctx context.Context, edge EdgeAPI, candidateAPI api.Candidate, fid string) error {
	var block []byte
	cid, err := getCID(edge, fid)
	if err != nil {
		if err == datastore.ErrNotFound {
			log.Infof("sendBlock, fid %s not found", fid)
			block = nil
		} else {
			return err
		}
	}

	if cid != "" {
		block, err = edge.blockStore.Get(cid)
		if err != nil {
			if err == datastore.ErrNotFound {
				log.Infof("sendBlock, cid %s not found, fid:%s", cid, fid)
				block = nil
			} else {
				log.Errorf("sendBlock, get block err:%v", err)
				return err
			}
		}
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

func (edge EdgeAPI) GetSchedulerAPI() api.Scheduler {
	log.Info("GetSchedulerAPI")
	return edge.scheduler
}
