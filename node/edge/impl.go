package edge

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"strings"
	"sync"
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

func NewLocalEdgeNode(ctx context.Context, params *EdgeParams) api.Edge {
	addrs, err := build.BuiltinBootstrap()
	if err != nil {
		log.Fatal(err)
	}

	exchange, err := p2p.Bootstrap(ctx, addrs)
	if err != nil {
		log.Fatal(err)
	}

	params.Device.DownloadSrvURL = parseDownloadSrvURL(params)
	params.Device.Limiter = rate.NewLimiter(rate.Limit(params.Device.BandwidthUp), int(params.Device.BandwidthUp))
	edge := &Edge{
		ds:             params.DS,
		scheduler:      params.Scheduler,
		blockStore:     params.BlockStore,
		exchange:       exchange,
		Device:         *params.Device,
		isCandidate:    params.IsCandidate,
		downloadSrvKey: params.DownloadSrvKey,
	}

	go edge.startBlockLoader()
	go edge.startDownloadServer(params.DownloadSrvAddr)

	return edge
}

func parseDownloadSrvURL(params *EdgeParams) string {
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
	Device          *device.Device
	IsCandidate     bool
	DownloadSrvKey  string
	DownloadSrvAddr string
}

type Edge struct {
	common.CommonAPI
	device.Device
	ds              datastore.Batching
	blockStore      stores.BlockStore
	scheduler       api.Scheduler
	exchange        exchange.Interface
	isCandidate     bool
	downloadSrvKey  string
	reqList         []delayReq
	cachingList     []delayReq
	cacheResultLock *sync.Mutex
}

func (edge *Edge) GetSchedulerAPI() api.Scheduler {
	log.Info("GetSchedulerAPI")
	return edge.scheduler
}

func (edge *Edge) WaitQuiet(ctx context.Context) error {
	log.Info("WaitQuiet")
	return nil
}

func (edge *Edge) CacheData(ctx context.Context, req api.ReqCacheData) error {
	log.Infof("CacheData, req len:%d", len(req.Cids))
	if edge.blockStore == nil {
		return fmt.Errorf("CacheData, blockStore == nil ")
	}

	// cache data for candidate
	delayReq := filterAvailableReq(edge, apiReq2DelayReq(&req))
	if len(delayReq) == 0 {
		log.Infof("CacheData, len(req) == 0 not need to handle")
		return nil
	}

	edge.reqList = append(edge.reqList, delayReq...)

	return nil
}

func (edge *Edge) BlockStoreStat(ctx context.Context) error {
	log.Info("BlockStoreStat")

	return nil
}

func (edge *Edge) LoadData(ctx context.Context, cid string) ([]byte, error) {
	log.Info("LoadData")

	if edge.blockStore == nil {
		log.Errorf("LoadData, blockStore not setting")
		return nil, nil
	}

	return edge.blockStore.Get(cid)
}

func (edge *Edge) DoValidate(ctx context.Context, reqValidate api.ReqValidate, candidateTcpSrvAddr string) error {
	log.Info("DoValidate")

	if edge.blockStore == nil {
		log.Errorf("DoValidate,edge.blockStore == nil ")
		return fmt.Errorf("edge.blockStore == nil")
	}

	fids := reqValidate.FIDs

	if len(fids) == 0 {
		log.Errorf("len(fids) == 0")
		return fmt.Errorf("len(fids) == 0")
	}

	oldRate := limitBlockUploadRate(edge)
	defer resetBlockUploadRate(edge, oldRate)

	conn, err := newTcpClient(candidateTcpSrvAddr)
	if err != nil {
		log.Errorf("DoValidate, NewCandicate err:%v", err)
		return err
	}

	go sendBlocks(conn, edge, &reqValidate)

	return nil
}

func sendBlocks(conn *net.TCPConn, edge *Edge, reqValidate *api.ReqValidate) {
	defer conn.Close()

	fids := reqValidate.FIDs
	r := rand.New(rand.NewSource(reqValidate.Seed))
	t := time.NewTimer(time.Duration(reqValidate.Duration) * time.Second)

	sendDeviceID(conn, edge.DeviceID)
	for {
		select {
		case <-t.C:
			return
		default:
		}

		random := r.Intn(len(fids))
		fidStr := fids[random]
		block, err := getBlock(edge, fidStr)
		if err != nil && err != datastore.ErrNotFound {
			log.Errorf("sendBlocks, get block error:%v", err)
			return
		}

		err = sendData(conn, block)
		if err != nil {
			log.Errorf("sendBlocks, send data error:%v", err)
			return
		}
	}
}

func limitBlockUploadRate(edge *Edge) int64 {
	if edge.Limiter == nil {
		log.Fatal("edge.Limiter == nil ")
	}

	oldRate := edge.Limiter.Limit()

	edge.Limiter.SetLimit(rate.Limit(0))
	edge.Limiter.SetBurst(int(0))

	return int64(oldRate)
}

func resetBlockUploadRate(edge *Edge, oldRate int64) {
	if edge.Limiter == nil {
		log.Errorf("edge.Limiter == nil")
	}

	edge.Limiter.SetLimit(rate.Limit(oldRate))
	edge.Limiter.SetBurst(int(oldRate))
}

func getBlock(edge *Edge, fid string) ([]byte, error) {
	cid, err := getCID(edge, fid)
	if err != nil {
		return nil, err
	}

	return edge.blockStore.Get(cid)
}

// call by scheduler
func (edge *Edge) DeleteData(ctx context.Context, cids []string) (api.DelResult, error) {
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
func (edge *Edge) DeleteBlocks(ctx context.Context, cids []string) (api.DelResult, error) {
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

func (edge *Edge) QueryCacheStat(ctx context.Context) (api.CacheStat, error) {
	result := api.CacheStat{}

	keyCount, err := edge.blockStore.KeyCount()
	if err != nil {
		log.Errorf("block store key count error:%v", err)
	}

	result.CacheBlockCount = keyCount
	result.WaitCacheBlockNum = len(edge.reqList)
	result.DoingCacheBlockNum = len(edge.cachingList)

	log.Infof("CacheBlockCount:%d,WaitCacheBlockNum:%d, DoingCacheBlockNum:%d", result.CacheBlockCount, result.WaitCacheBlockNum, result.DoingCacheBlockNum)
	return result, nil
}

func (edge *Edge) QueryCachingBlocks(ctx context.Context) (api.CachingBlockList, error) {
	result := api.CachingBlockList{}
	return result, nil
}

func (edge *Edge) SetDownloadSpeed(ctx context.Context, speedRate int64) error {
	log.Infof("set download speed %d", speedRate)
	if edge.Limiter == nil {
		return fmt.Errorf("edge.limiter == nil")
	}
	edge.Limiter.SetLimit(rate.Limit(speedRate))
	edge.Limiter.SetBurst(int(speedRate))

	return nil
}

func (edge *Edge) UnlimitDownloadSpeed(ctx context.Context) error {
	log.Infof("UnlimitDownloadSpeed")
	if edge.Limiter == nil {
		return fmt.Errorf("edge.limiter == nil")
	}

	edge.Limiter.SetLimit(rate.Inf)
	edge.Limiter.SetBurst(0)

	return nil
}

func (edge *Edge) GenerateDownloadToken(ctx context.Context) (string, error) {
	log.Infof("GenerateDownloadToken")
	return token.GenerateToken(edge.downloadSrvKey, time.Now().Add(30*24*time.Hour).Unix())
}
