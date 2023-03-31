package carfile

import (
	"context"
	"fmt"

	"github.com/ipfs/go-cid"
	legacy "github.com/ipfs/go-ipld-legacy"
	"github.com/ipfs/go-libipfs/blocks"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipfs/go-merkledag"
	dagpb "github.com/ipld/go-codec-dagpb"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/api/types"
	"github.com/linguohua/titan/node/carfile/cache"
	"github.com/linguohua/titan/node/carfile/storage"
	mh "github.com/multiformats/go-multihash"
)

var log = logging.Logger("carfile")

type CarfileImpl struct {
	scheduler       api.Scheduler
	cacheMgr        *cache.Manager
	TotalBlockCount int
}

func NewCarfileImpl(storageMgr *storage.Manager, scheduler api.Scheduler, cacheMgr *cache.Manager) *CarfileImpl {
	legacy.RegisterCodec(cid.DagProtobuf, dagpb.Type.PBNode, merkledag.ProtoNodeConverter)
	legacy.RegisterCodec(cid.Raw, basicnode.Prototype.Bytes, merkledag.RawNodeConverter)

	return &CarfileImpl{
		scheduler: scheduler,
		cacheMgr:  cacheMgr,
	}
}

func (cfImpl *CarfileImpl) CacheCarfile(ctx context.Context, rootCID string, dss []*types.DownloadSource) error {
	if types.RunningNodeType == types.NodeEdge && len(dss) == 0 {
		return fmt.Errorf("download source can not empty")
	}

	root, err := cid.Decode(rootCID)
	if err != nil {
		return err
	}

	has, err := cfImpl.cacheMgr.HasCar(root)
	if err != nil {
		return err
	}

	if has {
		log.Debugf("CacheCarfile %s already exist", root.String())
		return nil
	}

	log.Debugf("CacheCarfile cid:%s", rootCID)

	cfImpl.cacheMgr.AddToWaitList(root, dss)
	return nil
}

func (cfImpl *CarfileImpl) DeleteCarfile(ctx context.Context, carfileCID string) error {
	c, err := cid.Decode(carfileCID)
	if err != nil {
		log.Errorf("DeleteCarfile, decode carfile cid %s error :%s", carfileCID, err.Error())
		return err
	}

	go func() {
		if err := cfImpl.cacheMgr.DeleteCar(c); err != nil {
			log.Errorf("delete car failed %s", err.Error())
			return
		}

		_, diskUsage := cfImpl.cacheMgr.GetDiskUsageStat()
		ret := types.RemoveAssetResult{BlocksCount: cfImpl.TotalBlockCount, DiskUsage: diskUsage}

		if err := cfImpl.scheduler.RemoveAssetResult(context.Background(), ret); err != nil {
			log.Errorf("remove asset result failed %s", err.Error())
		}
	}()

	return nil
}

func (cfImpl *CarfileImpl) QueryCacheStat(ctx context.Context) (*types.CacheStat, error) {
	carfileCount, err := cfImpl.cacheMgr.CountCar()
	if err != nil {
		return nil, err
	}

	cacheStat := &types.CacheStat{}
	cacheStat.TotalBlockCount = cfImpl.TotalBlockCount
	cacheStat.TotalAssetCount = carfileCount
	cacheStat.WaitCacheAssetCount = cfImpl.cacheMgr.WaitListLen()
	_, cacheStat.DiskUsage = cfImpl.cacheMgr.GetDiskUsageStat()

	carfileCache := cfImpl.cacheMgr.CachingCar()
	if carfileCache != nil {
		cacheStat.CachingAssetCID = carfileCache.Root().String()
	}

	log.Debugf("cacheStat:%#v", *cacheStat)

	return cacheStat, nil
}

func (cfImpl *CarfileImpl) QueryCachingCarfile(ctx context.Context) (*types.CachingAsset, error) {
	carfileCache := cfImpl.cacheMgr.CachingCar()
	if carfileCache == nil {
		return nil, fmt.Errorf("caching carfile not exist")
	}

	ret := &types.CachingAsset{}
	ret.CID = carfileCache.Root().Hash().String()
	ret.TotalSize = carfileCache.TotalSize()
	ret.DoneSize = carfileCache.DoneSize()

	return ret, nil
}

func (cfImpl *CarfileImpl) GetBlocksOfCarfile(carfileCID string, indices []int) (map[int]string, error) {
	root, err := cid.Decode(carfileCID)
	if err != nil {
		return nil, err
	}

	return cfImpl.cacheMgr.GetBlocksOfCarfile(root, indices)
}

func (cfImpl *CarfileImpl) BlockCountOfCarfile(carfileCID string) (int, error) {
	c, err := cid.Decode(carfileCID)
	if err != nil {
		return 0, err
	}

	count, err := cfImpl.cacheMgr.BlockCountOfCar(context.Background(), c)
	if err != nil {
		return 0, err
	}

	return int(count), nil
}

func (cfImpl *CarfileImpl) CacheCarForSyncData(carfiles []string) error {
	switch types.RunningNodeType {
	case types.NodeCandidate:
		for _, hash := range carfiles {
			multihash, err := mh.FromHexString(hash)
			if err != nil {
				return err
			}

			cid := cid.NewCidV1(cid.Raw, multihash)
			if err := cfImpl.CacheCarfile(context.Background(), cid.String(), nil); err != nil {
				return err
			}
		}
	case types.NodeEdge:
		return fmt.Errorf("not implement")
	default:
		return fmt.Errorf("unsupport node type:%s", types.RunningNodeType)
	}

	return nil
}

func (cfImpl *CarfileImpl) CachedProgresses(ctx context.Context, carfileCIDs []string) (*types.PullResult, error) {
	progresses := make([]*types.AssetPullProgress, 0, len(carfileCIDs))
	for _, carfileCID := range carfileCIDs {
		root, err := cid.Decode(carfileCID)
		if err != nil {
			return nil, err
		}

		progress, err := cfImpl.carProgress(root)
		if err != nil {
			return nil, err
		}

		progresses = append(progresses, progress)
	}

	result := &types.PullResult{
		Progresses:       progresses,
		TotalBlocksCount: cfImpl.TotalBlockCount,
	}

	if count, err := cfImpl.cacheMgr.CountCar(); err == nil {
		result.AssetCount = count
	}
	_, result.DiskUsage = cfImpl.cacheMgr.GetDiskUsageStat()

	return result, nil
}

func (cfImpl *CarfileImpl) progressForSucceededCar(root cid.Cid) (*types.AssetPullProgress, error) {
	progress := &types.AssetPullProgress{
		CID:    root.String(),
		Status: types.ReplicaStatusSucceeded,
	}

	if count, err := cfImpl.cacheMgr.BlockCountOfCar(context.Background(), root); err == nil {
		progress.BlocksCount = int(count)
		progress.DoneBlocksCount = int(count)
	}

	blk, err := cfImpl.cacheMgr.GetBlock(context.Background(), root)
	if err != nil {
		return nil, err
	}

	blk = blocks.NewBlock(blk.RawData())
	node, err := legacy.DecodeNode(context.Background(), blk)
	if err != nil {
		return nil, err
	}

	linksSize := uint64(len(blk.RawData()))
	for _, link := range node.Links() {
		linksSize += link.Size
	}

	progress.Size = int64(linksSize)
	progress.DoneSize = int64(linksSize)

	return progress, nil
}

func (cfImpl *CarfileImpl) carProgress(root cid.Cid) (*types.AssetPullProgress, error) {
	status, err := cfImpl.cacheMgr.CachedStatus(root)
	if err != nil {
		return nil, err
	}

	switch status {
	case types.ReplicaStatusWaiting:
		return &types.AssetPullProgress{CID: root.String(), Status: types.ReplicaStatusWaiting}, nil
	case types.ReplicaStatusPulling:
		return cfImpl.cacheMgr.CachingCar().Progress(), nil
	case types.ReplicaStatusFailed:
		return cfImpl.cacheMgr.ProgressForFailedCar(root)
	case types.ReplicaStatusSucceeded:
		return cfImpl.progressForSucceededCar(root)
	}
	return nil, nil
}
