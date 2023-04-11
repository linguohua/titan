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
	assetPullerDir = "asset-puller"
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
// Manager handles storage operations
type Manager struct {
	baseDir     string
	asset       *asset
	wl          *waitList
	assetPuller *assetPuller
	count       *count
	assetsView  *AssetsView
}

// ManagerOptions contains configuration options for the Manager
type ManagerOptions struct {
	AssetPullerDir   string
	waitListFilePath string
	AssetsDir        string
	AssetSuffix      string
	CountDir         string
	AssetsViewDir    string
	// data view size of buckets
	BucketSize uint32
}

// NewManager creates a new Manager instance
func NewManager(baseDir string, opts *ManagerOptions) (*Manager, error) {
	if opts == nil {
		opts = defaultOptions(baseDir)
	}

	asset, err := newAsset(opts.AssetsDir, opts.AssetSuffix)
	if err != nil {
		return nil, err
	}

	assetPuller, err := newAssetPuller(opts.AssetPullerDir)
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
		baseDir:     baseDir,
		asset:       asset,
		assetsView:  assetsView,
		wl:          waitList,
		assetPuller: assetPuller,
		count:       count,
	}, nil
}

// defaultOptions generates default configuration options
func defaultOptions(baseDir string) *ManagerOptions {
	opts := &ManagerOptions{
		AssetPullerDir:   filepath.Join(baseDir, assetPullerDir),
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

// StoreAssetPuller stores an asset in the asset puller
func (m *Manager) PutAssetPuller(c cid.Cid, data []byte) error {
	return m.assetPuller.put(c, data)
}

// RetrieveAssetPuller retrieves an asset from the asset puller
func (m *Manager) GetAssetPuller(c cid.Cid) ([]byte, error) {
	return m.assetPuller.get(c)
}

// AssetPullerExists checks if an asset exists in the asset puller
func (m *Manager) HasAssetPuller(c cid.Cid) (bool, error) {
	return m.assetPuller.has(c)
}

// DeleteAssetPuller removes an asset from the asset puller
func (m *Manager) RemoveAssetPuller(c cid.Cid) error {
	return m.assetPuller.delete(c)
}

// asset api
// StoreBlocks stores multiple blocks for an asset
func (m *Manager) PutBlocks(ctx context.Context, root cid.Cid, blks []blocks.Block) error {
	return m.asset.putBlocks(ctx, root, blks)
}

// StoreAsset stores a single asset
func (m *Manager) PutAsset(ctx context.Context, root cid.Cid) error {
	return m.asset.putAsset(ctx, root)
}

// RetrieveAsset retrieves an asset
func (m *Manager) GetAsset(root cid.Cid) (io.ReadSeekCloser, error) {
	return m.asset.get(root)
}

// AssetExists checks if an asset exists
func (m *Manager) HasAsset(root cid.Cid) (bool, error) {
	return m.asset.has(root)
}

// DeleteAsset removes an asset
func (m *Manager) RemoveAsset(root cid.Cid) error {
	return m.asset.remove(root)
}

// AssetCount returns the number of assets
func (m *Manager) CountAsset() (int, error) {
	return m.asset.count()
}

// GetBlockCount retrieves the block count of an asset
func (m *Manager) BlockCountOfAsset(ctx context.Context, root cid.Cid) (uint32, error) {
	return m.count.get(ctx, root)
}

// SetBlockCount sets the block count of an asset
func (m *Manager) SetBlockCountOfAsset(ctx context.Context, root cid.Cid, count uint32) error {
	return m.count.put(ctx, root, count)
}

// AssetsView API
// FetchTopHash retrieves the top hash of assets
func (m *Manager) GetTopHash(ctx context.Context) (string, error) {
	return m.assetsView.getTopHash(ctx)
}

// FetchBucketHashes retrieves the hashes for each bucket
func (m *Manager) GetBucketHashes(ctx context.Context) (map[uint32]string, error) {
	return m.assetsView.getBucketHashes(ctx)
}

// FetchAssetsInBucket retrieves the assets in a specific bucket
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

// AddAssetToView adds an asset to the assets view
func (m *Manager) AddAssetToAssetsView(ctx context.Context, root cid.Cid) error {
	return m.assetsView.addAsset(ctx, root)
}

// RemoveAssetFromView removes an asset from the assets view
func (m *Manager) RemoveAssetFromAssetsView(ctx context.Context, root cid.Cid) error {
	return m.assetsView.removeAsset(ctx, root)
}

// WaitList API

// StoreWaitList stores the waitlist data
func (m *Manager) PutWaitList(data []byte) error {
	return m.wl.put(data)
}

// FetchWaitList retrieves the waitlist data
func (m *Manager) GetWaitList() ([]byte, error) {
	return m.wl.get()
}

// DiskStat API

// FetchDiskUsage retrieves the disk usage statistics
func (m *Manager) GetDiskUsageStat() (totalSpace, usage float64) {
	usageStat, err := disk.Usage(m.baseDir)
	if err != nil {
		log.Errorf("get disk usage stat error: %s", err)
		return 0, 0
	}
	return float64(usageStat.Total), usageStat.UsedPercent
}

// GetFileSystemType retrieves the type of the file system
func (m *Manager) GetFilesystemType() string {
	return "not implement"
}
