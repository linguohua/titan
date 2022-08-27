package edge

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/build"
	"github.com/linguohua/titan/lib/p2p"
	"github.com/linguohua/titan/lib/token"
	"github.com/linguohua/titan/stores"
	"golang.org/x/time/rate"

	"github.com/ipfs/go-datastore"
	exchange "github.com/ipfs/go-ipfs-exchange-interface"
	"github.com/linguohua/titan/node/common"
	"github.com/linguohua/titan/node/device"
)

func NewLocalEdgeNode(ctx context.Context, params EdgeParams) api.Edge {
	addrs, err := build.BuiltinBootstrap()
	if err != nil {
		log.Fatal(err)
	}

	exchange, err := p2p.Bootstrap(ctx, addrs)
	if err != nil {
		log.Fatal(err)
	}

	params.Device.DownloadSrvURL = parseDownloadSrvURL(params)
	edge := EdgeAPI{
		ds:             params.DS,
		scheduler:      params.Scheduler,
		blockStore:     params.BlockStore,
		limiter:        rate.NewLimiter(rate.Inf, 0),
		exchange:       exchange,
		DeviceAPI:      params.Device,
		isCandidate:    params.IsCandidate,
		downloadSrvKey: params.DownloadSrvKey,
	}

	go startBlockLoader(ctx, edge)
	go edge.startDownloadServer(params.DownloadSrvAddr)

	return edge
}

func parseDownloadSrvURL(params EdgeParams) string {
	const unspecifiedAddress = "0.0.0.0"
	addressSlice := strings.Split(params.DownloadSrvAddr, ":")
	if len(addressSlice) != 2 {
		log.Fatal("Invalid downloadSrvAddr")
	}

	if addressSlice[0] == unspecifiedAddress {
		return fmt.Sprintf("http://%s:%s%s", params.Device.InternalIP, addressSlice[1], downloadSrvPath)
	}

	return fmt.Sprintf("http://%s%s", params.DownloadSrvAddr, downloadSrvPath)
}

type EdgeParams struct {
	DS              datastore.Batching
	Scheduler       api.Scheduler
	BlockStore      stores.BlockStore
	Device          device.DeviceAPI
	IsCandidate     bool
	DownloadSrvKey  string
	DownloadSrvAddr string
}

type EdgeAPI struct {
	common.CommonAPI
	device.DeviceAPI
	ds             datastore.Batching
	blockStore     stores.BlockStore
	scheduler      api.Scheduler
	limiter        *rate.Limiter
	exchange       exchange.Interface
	isCandidate    bool
	downloadSrvKey string
}

func (edge EdgeAPI) GetSchedulerAPI() api.Scheduler {
	log.Info("GetSchedulerAPI")
	return edge.scheduler
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

	// cache data for candidate
	delayReq := filterAvailableReq(edge, apiReq2DelayReq(req))
	if len(delayReq) == 0 {
		log.Infof("CacheData, len(req) == 0 not need to handle")
		return nil
	}

	reqList = append(reqList, delayReq...)

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

	fids := reqVerify.FIDs

	if len(fids) == 0 {
		log.Errorf("len(fids) == 0")
		return fmt.Errorf("len(fids) == 0")
	}

	conn, err := newTcpClient(candidateURL)
	if err != nil {
		log.Errorf("DoVerify, NewCandicate err:%v", err)
		return err
	}
	defer conn.Close()

	r := rand.New(rand.NewSource(reqVerify.Seed))
	t := time.NewTimer(time.Duration(reqVerify.Duration) * time.Second)

	sendDeviceID(conn, edge.DeviceID)
	for {
		select {
		case <-t.C:
			return nil
		default:
		}

		random := r.Intn(len(fids))
		fidStr := fids[random]
		block, err := getBlock(ctx, edge, fidStr)
		if err != nil {
			return err
		}

		err = sendData(conn, block)
		if err != nil {
			return err
		}
	}
}

func getBlock(ctx context.Context, edge EdgeAPI, fid string) ([]byte, error) {
	var block []byte
	cid, err := getCID(edge, fid)
	if err != nil {
		if err == datastore.ErrNotFound {
			log.Infof("getBlock, fid %s not found", fid)
			block = nil
		} else {
			return nil, err
		}
	}

	if cid != "" {
		block, err = edge.blockStore.Get(cid)
		if err != nil {
			if err == datastore.ErrNotFound {
				log.Infof("getBlock, cid %s not found, fid:%s", cid, fid)
				block = nil
			} else {
				log.Errorf("getBlock, get block err:%v", err)
				return nil, err
			}
		}
	}
	// log.Infof("getBlock, fid:%s, cid:%s", fid, cid)
	return block, nil
}

// call by scheduler
func (edge EdgeAPI) DeleteData(ctx context.Context, cids []string) (api.DelResult, error) {
	log.Info("DeleteData")
	delResult := api.DelResult{}
	delResult.List = make([]api.DelFailed, 0)

	if edge.blockStore == nil {
		log.Errorf("DeleteData, blockStore not setting")
		return delResult, fmt.Errorf("edge.blockStore == nil")
	}

	for _, cid := range cids {
		err := edge.blockStore.Delete(cid)
		if err == datastore.ErrNotFound {
			continue
		}

		if err != nil {
			result := api.DelFailed{Cid: cid, ErrMsg: err.Error()}
			delResult.List = append(delResult.List, result)
			log.Errorf("DeleteData, delete block %s error:%v", cid, err)
			continue
		}

		fid, err := getFID(edge, cid)
		if err != nil {
			log.Errorf("DeleteData, get fid from cid %s error:%v", cid, err)
			continue
		}

		err = edge.ds.Delete(ctx, newKeyFID(fid))
		if err != nil {
			log.Errorf("DeleteData, delete key fid %s error:%v", fid, err)
		}
		err = edge.ds.Delete(ctx, newKeyCID(cid))
		if err != nil {
			log.Errorf("DeleteData, delete key cid %s error:%v", cid, err)
		}
	}
	return delResult, nil
}

// call by edge or candidate
func (edge EdgeAPI) DeleteBlocks(ctx context.Context, cids []string) (api.DelResult, error) {
	log.Info("DeleteBlock")
	delResult := api.DelResult{}
	delResult.List = make([]api.DelFailed, 0)

	result, err := edge.scheduler.DeleteDataRecord(ctx, edge.DeviceID, cids)
	if err != nil {
		log.Errorf("DeleteBlock, delete block error:%v", err)
		return delResult, err
	}

	for _, cid := range cids {
		_, ok := result[cid]
		if ok {
			continue
		}

		err = edge.blockStore.Delete(cid)
		if err != nil {
			result[cid] = err.Error()
		}

		fid, err := getFID(edge, cid)
		if err != nil {
			log.Errorf("DeleteData, get fid from cid %s error:%v", cid, err)
			continue
		}

		err = edge.ds.Delete(ctx, newKeyFID(fid))
		if err != nil {
			log.Errorf("DeleteData, delete key fid %s error:%v", fid, err)
		}
		err = edge.ds.Delete(ctx, newKeyCID(cid))
		if err != nil {
			log.Errorf("DeleteData, delete key cid %s error:%v", cid, err)
		}
	}

	for k, v := range result {
		log.Errorf("DeleteBlock, delete block %s error:%v", k, v)
		result := api.DelFailed{Cid: k, ErrMsg: v}
		delResult.List = append(delResult.List, result)
	}

	return delResult, nil
}

func (edge EdgeAPI) QueryCacheStat(ctx context.Context) (api.CacheStat, error) {
	result := api.CacheStat{}

	keyCount, err := edge.blockStore.KeyCount()
	if err != nil {
		log.Errorf("block store key count error:%v", err)
	}

	result.CacheBlockCount = keyCount
	result.WaitCacheBlockNum = len(reqList)
	result.DoingCacheBlockNum = len(cachingList)

	log.Infof("CacheBlockCount:%d,WaitCacheBlockNum:%d, DoingCacheBlockNum:%d", result.CacheBlockCount, result.WaitCacheBlockNum, result.DoingCacheBlockNum)
	return result, nil
}

func (edge EdgeAPI) QueryCachingBlocks(ctx context.Context) (api.CachingBlockList, error) {
	result := api.CachingBlockList{}
	return result, nil
}

func (edge EdgeAPI) SetDownloadSpeed(ctx context.Context, speedRate int64) error {
	log.Infof("set download speed %d", speedRate)
	if edge.limiter == nil {
		return fmt.Errorf("edge.limiter == nil")
	}
	edge.limiter.SetLimit(rate.Limit(speedRate))
	edge.limiter.SetBurst(int(speedRate))

	return nil
}

func (edge EdgeAPI) UnlimitDownloadSpeed(ctx context.Context) error {
	log.Infof("UnlimitDownloadSpeed")
	if edge.limiter == nil {
		return fmt.Errorf("edge.limiter == nil")
	}

	edge.limiter.SetLimit(rate.Inf)
	edge.limiter.SetBurst(0)

	return nil
}

func (edge EdgeAPI) GenerateDownloadToken(ctx context.Context) (string, error) {
	log.Infof("GenerateDownloadToken")
	return token.GenerateToken(edge.downloadSrvKey, time.Now().Add(30*24*time.Hour).Unix())
}
