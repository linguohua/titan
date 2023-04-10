package storage

import (
	"context"
	"io"
	"path/filepath"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-libipfs/blocks"
	logging "github.com/ipfs/go-log/v2"
	"github.com/shirou/gopsutil/v3/disk"
)

var log = logging.Logger("asset/store")

const (
	// dir of file name
	assetCacheDir  = "asset-cache"
	waitListFile   = "wait-list"
	assetsDir      = "assets"
	transientsDir  = "tmp"
	countDir       = "count"
	assetSuffix    = ".car"
	assetsViewDir  = "assets-view"
	maxSizeOfCache = 1024
	sizeOfBucket   = 128
)

// Manager access operation of storage
type Manager struct {
	baseDir    string
	asset      *asset
	wl         *waitList
	assetCache *assetCache
	count      *count
	assetsView *AssetsView
}

type ManagerOptions struct {
	AssetCacheDir    string
	waitListFilePath string
	AssetsDir        string
	AssetSuffix      string
	CountDir         string
	AssetsViewDir    string
	// data view size of buckets
	BucketSize uint32
}

func NewManager(baseDir string, opts *ManagerOptions) (*Manager, error) {
	if opts == nil {
		opts = defaultOptions(baseDir)
	}

	asset, err := newAsset(opts.AssetsDir, opts.AssetSuffix)
	if err != nil {
		return nil, err
	}

	assetCache, err := newAssetCache(opts.AssetCacheDir)
	if err != nil {
		return nil, err
	}

	count, err := newCount(opts.CountDir)
	if err != nil {
		return nil, err
	}

	assetsView, err := newAssetsView(opts.AssetsViewDir, opts.BucketSize)
	if err != nil {
		return nil, err
	}

	waitList := newWaitList(opts.waitListFilePath)
	return &Manager{
		baseDir:    baseDir,
		asset:      asset,
		assetsView: assetsView,
		wl:         waitList,
		assetCache: assetCache,
		count:      count,
	}, nil
}

func defaultOptions(baseDir string) *ManagerOptions {
	opts := &ManagerOptions{
		AssetCacheDir:    filepath.Join(baseDir, assetCacheDir),
		waitListFilePath: filepath.Join(baseDir, waitListFile),
		AssetsDir:        filepath.Join(baseDir, assetsDir),
		AssetSuffix:      assetSuffix,
		CountDir:         filepath.Join(baseDir, countDir),
		AssetsViewDir:    filepath.Join(baseDir, assetsViewDir),
		// cache for asset index
		BucketSize: sizeOfBucket,
	}
	return opts
}

// assetCache api
func (m *Manager) PutAssetCache(c cid.Cid, data []byte) error {
	return m.assetCache.put(c, data)
}

func (m *Manager) GetAssetCache(c cid.Cid) ([]byte, error) {
	return m.assetCache.get(c)
}

func (m *Manager) HasAssetCache(c cid.Cid) (bool, error) {
	return m.assetCache.has(c)
}

func (m *Manager) RemoveAssetCache(c cid.Cid) error {
	return m.assetCache.delete(c)
}

// asset api
func (m *Manager) PutBlocks(ctx context.Context, root cid.Cid, blks []blocks.Block) error {
	return m.asset.putBlocks(ctx, root, blks)
}

func (m *Manager) PutAsset(ctx context.Context, root cid.Cid) error {
	return m.asset.putAsset(ctx, root)
}

func (m *Manager) GetAsset(root cid.Cid) (io.ReadSeekCloser, error) {
	return m.asset.get(root)

}

func (m *Manager) HasAsset(root cid.Cid) (bool, error) {
	return m.asset.has(root)
}

func (m *Manager) RemoveAsset(root cid.Cid) error {
	return m.asset.remove(root)
}

func (m *Manager) CountAsset() (int, error) {
	return m.asset.count()
}

func (m *Manager) BlockCountOfAsset(ctx context.Context, root cid.Cid) (uint32, error) {
	return m.count.get(ctx, root)
}

func (m *Manager) SetBlockCountOfAsset(ctx context.Context, root cid.Cid, count uint32) error {
	return m.count.put(ctx, root, count)
}

// data view api
func (m *Manager) GetTopHash(ctx context.Context) (string, error) {
	return m.assetsView.getTopHash(ctx)
}
func (m *Manager) GetBucketHashes(ctx context.Context) (map[uint32]string, error) {
	return m.assetsView.getBucketHashes(ctx)
}
func (m *Manager) GetAssetsOfBucket(ctx context.Context, bucketID uint32) ([]cid.Cid, error) {
	hashes, err := m.assetsView.getAssetHashes(ctx, bucketID)
	if err != nil {
		return nil, err
	}

	cids := make([]cid.Cid, 0, len(hashes))
	for _, h := range hashes {
		cids = append(cids, cid.NewCidV0(h))
	}
	return cids, nil
}
func (m *Manager) AddAssetToAssetsView(ctx context.Context, root cid.Cid) error {
	return m.assetsView.addAsset(ctx, root)
}
func (m *Manager) RemoveAssetFromAssetsView(ctx context.Context, root cid.Cid) error {
	return m.assetsView.removeAsset(ctx, root)
}

// wait list
func (m *Manager) PutWaitList(data []byte) error {
	return m.wl.put(data)
}
func (m *Manager) GetWaitList() ([]byte, error) {
	return m.wl.get()
}

// disk stat
func (m *Manager) GetDiskUsageStat() (totalSpace, usage float64) {
	usageStat, err := disk.Usage(m.baseDir)
	if err != nil {
		log.Errorf("get disk usage stat error: %s", err)
		return 0, 0
	}
	return float64(usageStat.Total), usageStat.UsedPercent
}
func (m *Manager) GetFilesystemType() string {
	return "not implement"
}
