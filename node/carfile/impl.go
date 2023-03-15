package carfile

import (
	"context"
	"fmt"
	"time"

	"github.com/ipfs/go-cid"
	legacy "github.com/ipfs/go-ipld-legacy"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipfs/go-merkledag"
	dagpb "github.com/ipld/go-codec-dagpb"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/api/types"
	"github.com/linguohua/titan/node/carfile/cache"
	"github.com/linguohua/titan/node/carfile/fetcher"
	"github.com/linguohua/titan/node/carfile/store"
	"github.com/linguohua/titan/node/device"
	mh "github.com/multiformats/go-multihash"
)

var log = logging.Logger("carfile")

const (
	batch               = 5
	schedulerApiTimeout = 3
)

type CarfileImpl struct {
	scheduler       api.Scheduler
	device          *device.Device
	cm              *cache.Manager
	carfileStore    *store.CarfileStore
	TotalBlockCount int
}

func NewCarfileImpl(carfileStore *store.CarfileStore, scheduler api.Scheduler, bFetcher fetcher.BlockFetcher, device *device.Device) *CarfileImpl {
	cfImpl := &CarfileImpl{
		scheduler:    scheduler,
		device:       device,
		carfileStore: carfileStore,
	}

	opts := &cache.ManagerOptions{CarfileStore: carfileStore, BFetcher: bFetcher, CResulter: cfImpl, DownloadBatch: batch}
	cfImpl.cm = cache.NewManager(opts)

	totalBlockCount, err := carfileStore.BlockCount()
	if err != nil {
		log.Panicf("NewCarfileImpl block count error:%s", err.Error())
	}
	cfImpl.TotalBlockCount = totalBlockCount

	legacy.RegisterCodec(cid.DagProtobuf, dagpb.Type.PBNode, merkledag.ProtoNodeConverter)
	legacy.RegisterCodec(cid.Raw, basicnode.Prototype.Bytes, merkledag.RawNodeConverter)

	return cfImpl
}

func (cfImpl *CarfileImpl) CacheCarfile(ctx context.Context, rootCID string, dss []*types.DownloadSource) (*types.CacheCarfileResult, error) {
	if types.RunningNodeType == types.NodeEdge && len(dss) == 0 {
		return nil, fmt.Errorf("download source can not empty")
	}

	root, err := cid.Decode(rootCID)
	if err != nil {
		return nil, err
	}

	has, err := cfImpl.carfileStore.HasIncompleteCarfile(root)
	if err != nil {
		log.Errorf("CacheCarfile, HasCarfile error:%s, carfile hash :%s", err.Error(), root.Hash().String())
		return nil, err
	}

	if has {
		log.Debugf("cache incomplete carfile %s", root.Hash().String())

		cfImpl.cm.AddToWaitList(root, dss)
		return cfImpl.cacheCarfileResult()
	}

	has, err = cfImpl.carfileStore.HashCarfile(root)
	if err != nil {
		log.Errorf("CacheCarfile, HasCarfile error:%s, carfile hash :%s", err.Error(), root.Hash().String())
		return nil, err
	}

	if has {
		log.Debugf("carfile %s carfileCID aready exist, not need to cache", rootCID)

		err = cfImpl.cacheResultForCarfileExist(rootCID)
		if err != nil {
			log.Errorf("CacheCarfile, cacheResultForCarfileExist error:%s", err.Error())
		}

		return cfImpl.cacheCarfileResult()
	}

	cfImpl.cm.AddToWaitList(root, dss)

	log.Debugf("CacheCarfile carfile cid:%s", rootCID)
	return cfImpl.cacheCarfileResult()
}

func (cfImpl *CarfileImpl) deleteCarfile(ctx context.Context, c cid.Cid) error {
	defer func() {
		if count, err := cfImpl.carfileStore.BlockCount(); err == nil {
			cfImpl.TotalBlockCount = count
		}

		_, diskUsage := cfImpl.device.GetDiskUsageStat()
		ret := types.RemoveCarfileResult{BlocksCount: cfImpl.TotalBlockCount, DiskUsage: diskUsage}

		cfImpl.scheduler.RemoveCarfileResult(context.Background(), ret)
	}()

	ok, err := cfImpl.cm.DeleteCarFromWaitList(c)
	if err != nil {
		log.Errorf("DeleteCarfile, delete car %s from wait list error:%s", c.String(), err.Error())
		return err
	}

	if ok {
		log.Debugf("delete carfile %s from wait list", c.String())
		return nil
	}

	log.Debugf("delete carfile %s", c.String())

	err = cfImpl.carfileStore.DeleteCarfile(c)
	if err != nil {
		log.Errorf("delete carfile error: %s", err.Error())
	}
	return err
}

func (cfImpl *CarfileImpl) DeleteCarfile(ctx context.Context, carfileCID string) error {
	c, err := cid.Decode(carfileCID)
	if err != nil {
		log.Errorf("DeleteCarfile, decode carfile cid %s error :%s", carfileCID, err.Error())
		return err
	}

	go cfImpl.deleteCarfile(ctx, c)

	return nil
}

func (cfImpl *CarfileImpl) DeleteAllCarfiles(ctx context.Context) error {
	return fmt.Errorf("unimplement")
}

func (cfImpl *CarfileImpl) GetBlock(ctx context.Context, cidStr string) ([]byte, error) {
	c, err := cid.Decode(cidStr)
	if err != nil {
		return nil, err
	}

	blk, err := cfImpl.carfileStore.Block(c)
	if err != nil {
		return nil, err
	}
	return blk.RawData(), nil
}

func (cfImpl *CarfileImpl) QueryCacheStat(ctx context.Context) (*types.CacheStat, error) {
	carfileCount, err := cfImpl.carfileStore.CarfileCount()
	if err != nil {
		return nil, err
	}

	cacheStat := &types.CacheStat{}
	cacheStat.TotalBlockCount = cfImpl.TotalBlockCount
	cacheStat.TotalCarfileCount = carfileCount
	cacheStat.WaitCacheCarfileCount = cfImpl.cm.WaitListLen()
	_, cacheStat.DiskUsage = cfImpl.device.GetDiskUsageStat()

	carfileCache := cfImpl.cm.CachingCar()
	if carfileCache != nil {
		cacheStat.CachingCarfileCID = carfileCache.Root().String()
	}

	log.Debugf("cacheStat:%#v", *cacheStat)

	return cacheStat, nil
}

func (cfImpl *CarfileImpl) QueryCachingCarfile(ctx context.Context) (*types.CachingCarfile, error) {
	carfileCache := cfImpl.cm.CachingCar()
	if carfileCache == nil {
		return nil, fmt.Errorf("caching carfile not exist")
	}

	ret := &types.CachingCarfile{}
	ret.CarfileCID = carfileCache.Root().Hash().String()
	ret.TotalSize = carfileCache.TotalSize()
	ret.DoneSize = carfileCache.DoneSize()

	return ret, nil
}

func (cfImpl *CarfileImpl) GetBlocksOfCarfile(carfileCID string, indexs []int) (map[int]string, error) {
	c, err := cid.Decode(carfileCID)
	if err != nil {
		return nil, err
	}

	cids, err := cfImpl.carfileStore.BlocksOfCarfile(c)
	if err != nil {
		return nil, err
	}

	ret := make(map[int]string, len(indexs))
	for _, index := range indexs {
		if index >= len(cids) {
			return nil, fmt.Errorf("index is out of blocks count")
		}

		c := cids[index]
		ret[index] = c.String()
	}
	return ret, nil
}

func (cfImpl *CarfileImpl) BlockCountOfCarfile(carfileCID string) (int, error) {
	c, err := cid.Decode(carfileCID)
	if err != nil {
		return 0, err
	}

	return cfImpl.carfileStore.BlockCountOfCarfile(c)
}

// TODO: return all waitList car to scheduler
func (cfImpl *CarfileImpl) CacheResult(ret *types.CacheResult) error {
	_, diskUsage := cfImpl.device.GetDiskUsageStat()
	ret.DiskUsage = diskUsage
	ret.TotalBlocksCount = cfImpl.TotalBlockCount

	ctx, cancel := context.WithTimeout(context.Background(), schedulerApiTimeout*time.Second)
	defer cancel()

	log.Debugf("downloadResult, carfile:%#v", *ret)
	return cfImpl.scheduler.CacheResult(ctx, *ret)
}

func (cfImpl *CarfileImpl) cacheCarfileResult() (*types.CacheCarfileResult, error) {
	_, diskUsage := cfImpl.device.GetDiskUsageStat()

	carfileCount, err := cfImpl.carfileStore.CarfileCount()
	if err != nil {
		return nil, err
	}

	return &types.CacheCarfileResult{CacheCarfileCount: carfileCount, WaitCacheCarfileNum: cfImpl.cm.WaitListLen(), DiskUsage: diskUsage}, nil
}

func (cfImpl *CarfileImpl) cacheResultForCarfileExist(carfileCID string) error {
	_, diskUsage := cfImpl.device.GetDiskUsageStat()

	c, err := cid.Decode(carfileCID)
	if err != nil {
		return err
	}

	blocksCount, err := cfImpl.carfileStore.BlockCountOfCarfile(c)
	if err != nil {
		return err
	}

	blk, err := cfImpl.carfileStore.Block(c)
	if err != nil {
		return err
	}

	node, err := legacy.DecodeNode(context.Background(), blk)
	if err != nil {
		return err
	}

	linksSize := uint64(len(blk.RawData()))
	for _, link := range node.Links() {
		linksSize += link.Size
	}

	result := types.CacheResult{
		Status:             types.CacheStatusSucceeded,
		CarfileBlocksCount: blocksCount,
		DoneBlocksCount:    blocksCount,
		CarfileSize:        int64(linksSize),
		DoneSize:           int64(linksSize),
		CarfileHash:        c.Hash().String(),
		DiskUsage:          diskUsage,
		TotalBlocksCount:   cfImpl.TotalBlockCount,
	}

	ctx, cancel := context.WithTimeout(context.Background(), schedulerApiTimeout*time.Second)
	defer cancel()

	return cfImpl.scheduler.CacheResult(ctx, result)
}

func (cfImpl *CarfileImpl) Cache(carfiles []string) error {
	switch types.RunningNodeType {
	case types.NodeCandidate:
		for _, hash := range carfiles {
			multihash, err := mh.FromHexString(hash)
			if err != nil {
				return err
			}

			cid := cid.NewCidV1(cid.Raw, multihash)
			cfImpl.CacheCarfile(context.Background(), cid.String(), nil)
		}
	case types.NodeEdge:
		return fmt.Errorf("unimplement")
	default:
		return fmt.Errorf("unsupport node type:%s", types.RunningNodeType)
	}

	return nil
}
