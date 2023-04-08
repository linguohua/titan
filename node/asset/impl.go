package asset

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
	"github.com/linguohua/titan/node/asset/cache"
	"github.com/linguohua/titan/node/asset/storage"
	"golang.org/x/xerrors"
)

var log = logging.Logger("carfile")

type Asset struct {
	scheduler       api.Scheduler
	cacheMgr        *cache.Manager
	TotalBlockCount int
}

func NewAsset(storageMgr *storage.Manager, scheduler api.Scheduler, cacheMgr *cache.Manager) *Asset {
	legacy.RegisterCodec(cid.DagProtobuf, dagpb.Type.PBNode, merkledag.ProtoNodeConverter)
	legacy.RegisterCodec(cid.Raw, basicnode.Prototype.Bytes, merkledag.RawNodeConverter)

	return &Asset{
		scheduler: scheduler,
		cacheMgr:  cacheMgr,
	}
}

func (a *Asset) CacheAsset(ctx context.Context, rootCID string, dss []*types.AssetDownloadSource) error {
	if types.RunningNodeType == types.NodeEdge && len(dss) == 0 {
		return fmt.Errorf("download source can not empty")
	}

	root, err := cid.Decode(rootCID)
	if err != nil {
		return err
	}

	has, err := a.cacheMgr.HasCar(root)
	if err != nil {
		return err
	}

	if has {
		log.Debugf("CacheCarfile %s already exist", root.String())
		return nil
	}

	log.Debugf("Cache carfile cid:%s", rootCID)

	a.cacheMgr.AddToWaitList(root, dss)
	return nil
}

func (a *Asset) DeleteAsset(ctx context.Context, carfileCID string) error {
	c, err := cid.Decode(carfileCID)
	if err != nil {
		log.Errorf("DeleteCarfile, decode carfile cid %s error :%s", carfileCID, err.Error())
		return err
	}

	log.Debugf("DeleteCarfile %s", carfileCID)

	go func() {
		if err := a.cacheMgr.DeleteCar(c); err != nil {
			log.Errorf("delete car failed %s", err.Error())
			return
		}

		_, diskUsage := a.cacheMgr.GetDiskUsageStat()
		ret := types.RemoveAssetResult{BlocksCount: a.TotalBlockCount, DiskUsage: diskUsage}

		if err := a.scheduler.RemoveAssetResult(context.Background(), ret); err != nil {
			log.Errorf("remove asset result failed %s", err.Error())
		}
	}()

	return nil
}

func (a *Asset) QueryAssetStats(ctx context.Context) (*types.AssetStats, error) {
	carfileCount, err := a.cacheMgr.CountCar()
	if err != nil {
		return nil, err
	}

	assetStats := &types.AssetStats{}
	assetStats.TotalBlockCount = a.TotalBlockCount
	assetStats.TotalAssetCount = carfileCount
	assetStats.WaitCacheAssetCount = a.cacheMgr.WaitListLen()
	_, assetStats.DiskUsage = a.cacheMgr.GetDiskUsageStat()

	carfileCache := a.cacheMgr.CachingCar()
	if carfileCache != nil {
		assetStats.InProgressAssetCID = carfileCache.Root().String()
	}

	log.Debugf("cacheStat:%#v", *assetStats)

	return assetStats, nil
}

func (a *Asset) QueryCachingAsset(ctx context.Context) (*types.InProgressAsset, error) {
	carfileCache := a.cacheMgr.CachingCar()
	if carfileCache == nil {
		return nil, fmt.Errorf("caching carfile not exist")
	}

	ret := &types.InProgressAsset{}
	ret.CID = carfileCache.Root().Hash().String()
	ret.TotalSize = carfileCache.TotalSize()
	ret.DoneSize = carfileCache.DoneSize()

	return ret, nil
}

func (a *Asset) GetBlocksOfCar(carfileCID string, randomSeed int64, randomCount int) (map[int]string, error) {
	root, err := cid.Decode(carfileCID)
	if err != nil {
		return nil, err
	}

	return a.cacheMgr.GetBlocksOfCarfile(root, randomSeed, randomCount)
}

func (a *Asset) BlockCountOfCarfile(carfileCID string) (int, error) {
	c, err := cid.Decode(carfileCID)
	if err != nil {
		return 0, err
	}

	count, err := a.cacheMgr.BlockCountOfCar(context.Background(), c)
	if err != nil {
		return 0, err
	}

	return int(count), nil
}

func (a *Asset) AssetProgresses(ctx context.Context, carfileCIDs []string) (*types.PullResult, error) {
	progresses := make([]*types.AssetPullProgress, 0, len(carfileCIDs))
	for _, carfileCID := range carfileCIDs {
		root, err := cid.Decode(carfileCID)
		if err != nil {
			log.Errorf("decode cid %s", err.Error())
			return nil, err
		}

		progress, err := a.carProgress(root)
		if err != nil {
			log.Errorf("carProgress %s", err.Error())
			return nil, err
		}
		progresses = append(progresses, progress)
	}

	result := &types.PullResult{
		Progresses:       progresses,
		TotalBlocksCount: a.TotalBlockCount,
	}

	if count, err := a.cacheMgr.CountCar(); err == nil {
		result.AssetCount = count
	}
	_, result.DiskUsage = a.cacheMgr.GetDiskUsageStat()

	return result, nil
}

func (a *Asset) progressForSucceededCar(root cid.Cid) (*types.AssetPullProgress, error) {
	progress := &types.AssetPullProgress{
		CID:    root.String(),
		Status: types.ReplicaStatusSucceeded,
	}

	if count, err := a.cacheMgr.BlockCountOfCar(context.Background(), root); err == nil {
		progress.BlocksCount = int(count)
		progress.DoneBlocksCount = int(count)
	}

	blk, err := a.cacheMgr.GetBlock(context.Background(), root, root)
	if err != nil {
		return nil, xerrors.Errorf("get block %w", err)
	}

	blk = blocks.NewBlock(blk.RawData())
	node, err := legacy.DecodeNode(context.Background(), blk)
	if err != nil {
		return nil, xerrors.Errorf("decode node %w", err)
	}

	linksSize := uint64(len(blk.RawData()))
	for _, link := range node.Links() {
		linksSize += link.Size
	}

	progress.Size = int64(linksSize)
	progress.DoneSize = int64(linksSize)

	return progress, nil
}

func (a *Asset) carProgress(root cid.Cid) (*types.AssetPullProgress, error) {
	status, err := a.cacheMgr.CachedStatus(root)
	if err != nil {
		return nil, xerrors.Errorf("car %s cache status %w", root.Hash(), err)
	}

	switch status {
	case types.ReplicaStatusWaiting:
		return &types.AssetPullProgress{CID: root.String(), Status: types.ReplicaStatusWaiting}, nil
	case types.ReplicaStatusPulling:
		return a.cacheMgr.CachingCar().Progress(), nil
	case types.ReplicaStatusFailed:
		return a.cacheMgr.ProgressForFailedCar(root)
	case types.ReplicaStatusSucceeded:
		return a.progressForSucceededCar(root)
	}
	return nil, xerrors.Errorf("unknown car %s status %d", root.String(), status)
}
