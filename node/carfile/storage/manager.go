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
	dataViewDir    = "data-view"
	maxSizeOfCache = 1024
	sizeOfBucket   = 128
)

// Manager access operation of storage
type Manager struct {
	baseDir  string
	car      *car
	wl       *waitList
	carCache *carCache
	count    *count
	dataView *dataView
}

type ManagerOptions struct {
	CarCacheDir      string
	waitListFilePath string
	CarsDir          string
	CarSuffix        string
	CountDir         string
	DataViewDir      string
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

	dataView, err := newDataView(opts.DataViewDir, opts.BucketSize)

	waitList := newWaitList(opts.waitListFilePath)
	return &Manager{
		baseDir:  baseDir,
		car:      car,
		dataView: dataView,
		wl:       waitList,
		carCache: carCache,
		count:    count,
	}, nil
}

func defaultOptions(baseDir string) *ManagerOptions {
	opts := &ManagerOptions{
		CarCacheDir:      filepath.Join(baseDir, carCacheDir),
		waitListFilePath: filepath.Join(baseDir, waitListFile),
		CarsDir:          filepath.Join(baseDir, carsDir),
		CarSuffix:        filepath.Join(baseDir, carSuffix),
		CountDir:         filepath.Join(baseDir, countDir),
		DataViewDir:      filepath.Join(baseDir, dataViewDir),
		// cache for car index
		BucketSize: sizeOfBucket,
	}
	return opts
}

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

func (m *Manager) PutBlocks(ctx context.Context, root cid.Cid, blks []blocks.Block) error {
	return m.car.putBlocks(ctx, root, blks)
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
func (m *Manager) SetTopChecksum(ctx context.Context, checksum string) error {
	return m.dataView.setTopChecksum(ctx, checksum)
}
func (m *Manager) SetBucketsChecksums(ctx context.Context, checksums map[uint32]string) error {
	return m.dataView.setBucketsChecksums(ctx, checksums)
}
func (m *Manager) GetTopChecksum(ctx context.Context) (string, error) {
	return m.dataView.getTopChecksum(ctx)
}
func (m *Manager) GetBucketsChecksums(ctx context.Context) (map[uint32]string, error) {
	return m.dataView.getBucketsChecksums(ctx)
}
func (m *Manager) GetCarsOfBucket(ctx context.Context, bucketID uint32) ([]cid.Cid, error) {
	return m.dataView.getCars(ctx, bucketID)
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
