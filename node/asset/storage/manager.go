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

var log = logging.Logger("carfile/store")

const (
	// dir of file name
	carCacheDir    = "car-cache"
	waitListFile   = "wait-list"
	carsDir        = "cars"
	transientsDir  = "tmp"
	countDir       = "count"
	carSuffix      = ".car"
	assetsViewDir  = "assets-view"
	maxSizeOfCache = 1024
	sizeOfBucket   = 128
)

// Manager access operation of storage
type Manager struct {
	baseDir    string
	car        *car
	wl         *waitList
	carCache   *carCache
	count      *count
	assetsView *AssetsView
}

type ManagerOptions struct {
	CarCacheDir      string
	waitListFilePath string
	CarsDir          string
	CarSuffix        string
	CountDir         string
	AssetsViewDir    string
	// data view size of buckets
	BucketSize uint32
}

func NewManager(baseDir string, opts *ManagerOptions) (*Manager, error) {
	if opts == nil {
		opts = defaultOptions(baseDir)
	}

	car, err := newCar(opts.CarsDir, opts.CarSuffix)
	if err != nil {
		return nil, err
	}

	carCache, err := newCarCache(opts.CarCacheDir)
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
		car:        car,
		assetsView: assetsView,
		wl:         waitList,
		carCache:   carCache,
		count:      count,
	}, nil
}

func defaultOptions(baseDir string) *ManagerOptions {
	opts := &ManagerOptions{
		CarCacheDir:      filepath.Join(baseDir, carCacheDir),
		waitListFilePath: filepath.Join(baseDir, waitListFile),
		CarsDir:          filepath.Join(baseDir, carsDir),
		CarSuffix:        filepath.Join(baseDir, carSuffix),
		CountDir:         filepath.Join(baseDir, countDir),
		AssetsViewDir:    filepath.Join(baseDir, assetsViewDir),
		// cache for car index
		BucketSize: sizeOfBucket,
	}
	return opts
}

// carCache api
func (m *Manager) PutCarCache(c cid.Cid, data []byte) error {
	return m.carCache.put(c, data)
}

func (m *Manager) GetCarCache(c cid.Cid) ([]byte, error) {
	return m.carCache.get(c)
}

func (m *Manager) HasCarCache(c cid.Cid) (bool, error) {
	return m.carCache.has(c)
}

func (m *Manager) RemoveCarCache(c cid.Cid) error {
	return m.carCache.delete(c)
}

// car api
func (m *Manager) PutBlocks(ctx context.Context, root cid.Cid, blks []blocks.Block) error {
	return m.car.putBlocks(ctx, root, blks)
}

func (m *Manager) PutCar(ctx context.Context, root cid.Cid) error {
	return m.car.putCar(ctx, root)
}

func (m *Manager) GetCar(root cid.Cid) (io.ReadSeekCloser, error) {
	return m.car.get(root)

}

func (m *Manager) HasCar(root cid.Cid) (bool, error) {
	return m.car.has(root)
}

func (m *Manager) RemoveCar(root cid.Cid) error {
	return m.car.remove(root)
}

func (m *Manager) CountCar() (int, error) {
	return m.car.count()
}

func (m *Manager) BlockCountOfCar(ctx context.Context, root cid.Cid) (uint32, error) {
	return m.count.get(ctx, root)
}

func (m *Manager) SetBlockCountOfCar(ctx context.Context, root cid.Cid, count uint32) error {
	return m.count.put(ctx, root, count)
}

// data view api
func (m *Manager) GetTopHash(ctx context.Context) (string, error) {
	return m.assetsView.getTopHash(ctx)
}
func (m *Manager) GetBucketHashes(ctx context.Context) (map[uint32]string, error) {
	return m.assetsView.getBucketHashes(ctx)
}
func (m *Manager) GetCarsOfBucket(ctx context.Context, bucketID uint32) ([]cid.Cid, error) {
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
func (m *Manager) AddCarToAssetsView(ctx context.Context, root cid.Cid) error {
	return m.assetsView.addCar(ctx, root)
}
func (m *Manager) RemoveCarFromAssetsView(ctx context.Context, root cid.Cid) error {
	return m.assetsView.removeCar(ctx, root)
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
