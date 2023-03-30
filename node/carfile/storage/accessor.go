package storage

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-libipfs/blocks"
	logging "github.com/ipfs/go-log/v2"
	carv2 "github.com/ipld/go-car/v2"
	"github.com/ipld/go-car/v2/index"
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

// Accessor access operation of storage
type Accessor struct {
	car      *car
	topIndex *topIndex
	wl       *waitList
	carCache *carCache
	count    *count
}

func NewAccessor(baseDir string) (*Accessor, error) {
	car, err := newCar(filepath.Join(baseDir, carsDir), carSuffix, maxSizeOfCache)
	if err != nil {
		return nil, err
	}

	topIndex, err := newTopIndex(filepath.Join(baseDir, topIndexDir))
	if err != nil {
		return nil, err
	}

	carCache, err := newCarCache(filepath.Join(baseDir, carCacheDir))
	if err != nil {
		return nil, err
	}

	count, err := newCount(filepath.Join(baseDir, countDir))
	if err != nil {
		return nil, err
	}

	waitList := newWaitList(filepath.Join(baseDir, waitListFile))
	return &Accessor{
		car:      car,
		topIndex: topIndex,
		wl:       waitList,
		carCache: carCache,
		count:    count,
	}, nil
}

func (a *Accessor) PutCarCache(c cid.Cid, data []byte) error {
	return a.carCache.put(c, data)
}

func (a *Accessor) GetCarCache(c cid.Cid) ([]byte, error) {
	return a.carCache.get(c)
}

func (a *Accessor) HasCarCache(c cid.Cid) (bool, error) {
	return a.carCache.has(c)
}

func (a *Accessor) RemoveCarCache(c cid.Cid) error {
	return a.carCache.delete(c)
}

func (a *Accessor) PutBlocks(ctx context.Context, root cid.Cid, blks []blocks.Block) error {
	return a.car.putBlocks(ctx, root, blks)
}

func (a *Accessor) GetBlock(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	root, err := a.topIndex.findCar(ctx, c)
	if err != nil {
		return nil, err
	}
	return a.car.getBlock(ctx, *root, c)
}

func (a *Accessor) HasBlock(ctx context.Context, c cid.Cid) (bool, error) {
	root, err := a.topIndex.findCar(ctx, c)
	if err != nil {
		return false, err
	}
	return a.car.hasBlock(ctx, *root, c)
}

func (a *Accessor) GetCar(root cid.Cid) (io.ReadSeekCloser, error) {
	return a.car.get(root)

}

func (a *Accessor) HasCar(root cid.Cid) (bool, error) {
	return a.car.has(root)
}

func (a *Accessor) RemoveCar(root cid.Cid) error {
	idx, err := a.getCarIndex(root)
	if err != nil {
		return err
	}
	// remove top index
	err = a.topIndex.remove(idx)
	if err != nil {
		return err
	}

	// remove car file
	return a.car.remove(root)
}

func (a *Accessor) CountCar() (int, error) {
	return a.car.count()
}

func (a *Accessor) BlockCountOfCar(ctx context.Context, root cid.Cid) (uint32, error) {
	return a.count.get(ctx, root)
}

func (a *Accessor) SetBlockCountOfCar(ctx context.Context, root cid.Cid, count uint32) error {
	return a.count.put(ctx, root, count)
}

func (a *Accessor) AddTopIndex(ctx context.Context, root cid.Cid) error {
	idx, err := a.getCarIndex(root)
	if err != nil {
		return err
	}
	return a.topIndex.add(ctx, root, idx)
}

func (a *Accessor) PutWaitList(data []byte) error {
	return a.wl.put(data)
}

func (a *Accessor) GetWaitList() ([]byte, error) {
	return a.wl.get()
}

func (a *Accessor) getCarIndex(root cid.Cid) (index.Index, error) {
	reader, err := a.car.get(root)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	f, ok := reader.(*os.File)
	if !ok {
		return nil, fmt.Errorf("can not convert reader to file")
	}
	// Open the CARv2 file
	cr, err := carv2.NewReader(f)
	if err != nil {
		panic(err)
	}
	defer cr.Close()

	// Read and unmarshall index within CARv2 file.
	ir, err := cr.IndexReader()
	if err != nil {
		return nil, err
	}
	idx, err := index.ReadFrom(ir)
	if err != nil {
		return nil, err
	}
	return idx, nil
}
