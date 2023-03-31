package storage

import (
	"context"
	"io"
	"path/filepath"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-libipfs/blocks"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipld/go-car/v2/index"
	"github.com/shirou/gopsutil/v3/disk"
)

var log = logging.Logger("carfile/store")

const (
	// dir of file name
	carCacheDir    = "car-cache"
	waitListFile   = "wait-list"
	carsDir        = "cars"
	topIndexDir    = "top-index"
	transientsDir  = "tmp"
	countDir       = "count"
	carSuffix      = ".car"
	maxSizeOfCache = 1024
)

// Manager access operation of storage
type Manager struct {
	baseDir  string
	car      *car
	topIndex *topIndex
	wl       *waitList
	carCache *carCache
	count    *count
}

type ManagerOptions struct {
	CarCacheDir      string
	waitListFilePath string
	CarsDir          string
	CarSuffix        string
	TopIndexDir      string
	CountDir         string
	// cache for car index
	MaxSizeOfCache int
}

func NewManager(baseDir string, opts *ManagerOptions) (*Manager, error) {
	if opts == nil {
		opts = defaultOptions(baseDir)
	}

	car, err := newCar(opts.CarsDir, opts.CarSuffix)
	if err != nil {
		return nil, err
	}

	topIndex, err := newTopIndex(opts.TopIndexDir)
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

	waitList := newWaitList(opts.waitListFilePath)
	return &Manager{
		baseDir:  baseDir,
		car:      car,
		topIndex: topIndex,
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
		TopIndexDir:      filepath.Join(baseDir, topIndexDir),
		CountDir:         filepath.Join(baseDir, countDir),
		// cache for car index
		MaxSizeOfCache: maxSizeOfCache,
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

func (m *Manager) FindCars(ctx context.Context, block cid.Cid) ([]cid.Cid, error) {
	return m.topIndex.findCars(ctx, block)
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

func (m *Manager) AddTopIndex(ctx context.Context, root cid.Cid, idx index.Index) error {
	return m.topIndex.add(ctx, root, idx)
}
func (m *Manager) RemoveTopIndex(ctx context.Context, root cid.Cid, idx index.Index) error {
	return m.topIndex.remove(ctx, root, idx)
}

func (m *Manager) PutWaitList(data []byte) error {
	return m.wl.put(data)
}

func (m *Manager) GetWaitList() ([]byte, error) {
	return m.wl.get()
}

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
