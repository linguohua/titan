package storage

import (
	"context"
	"io"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-libipfs/blocks"
	logging "github.com/ipfs/go-log/v2"
)

var log = logging.Logger("carfile/store")

const (
	// dir of file name
	carCacheDir    = "car-cache"
	waitListFile   = "wait-list"
	carsDir        = "cars"
	topIndexDir    = "top-index"
	transientsDir  = "tmp"
	carSuffix      = ".car"
	maxSizeOfCache = 1024
)

// Accessor access operation of storage
type Accessor struct {
	car      *Car
	topIndex *TopIndex
	wl       *WaitList
	carCache *CarCache
}

func NewAccessor() (*Accessor, error) {
	car, err := NewCar(carsDir, carSuffix, maxSizeOfCache)
	if err != nil {
		return nil, err
	}

	topIndex, err := NewTopIndex(topIndexDir)
	if err != nil {
		return nil, err
	}

	carCache, err := NewCarCache(carCacheDir)
	if err != nil {
		return nil, err
	}

	waitList := NewWaitList(waitListFile)
	return &Accessor{car: car, topIndex: topIndex, wl: waitList, carCache: carCache}, nil
}

func (a *Accessor) PutCarCache(c cid.Cid, data []byte) error {
	return a.carCache.Put(c, data)
}

func (a *Accessor) GetCarCache(c cid.Cid) ([]byte, error) {
	return a.carCache.Get(c)
}

func (a *Accessor) HasCarCache(c cid.Cid) (bool, error) {
	return a.carCache.Has(c)
}

func (a *Accessor) DeleteCarCache(c cid.Cid) error {
	return a.carCache.Delete(c)
}

func (a *Accessor) PutBlocks(ctx context.Context, root cid.Cid, blks []blocks.Block) error {
	return a.car.PutBlocks(ctx, root, blks)
}

func (a *Accessor) GetBlock(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	root, err := a.topIndex.FindCar(ctx, c)
	if err != nil {
		return nil, err
	}
	return a.car.GetBlock(ctx, *root, c)
}

func (a *Accessor) HasBlock(ctx context.Context, c cid.Cid) (bool, error) {
	root, err := a.topIndex.FindCar(ctx, c)
	if err != nil {
		return false, err
	}
	return a.car.HasBlock(ctx, *root, c)
}

func (a *Accessor) GetCar(root cid.Cid) (io.ReadSeekCloser, error) {
	return a.car.Get(root)

}

func (a *Accessor) HasCar(root cid.Cid) (bool, error) {
	return a.car.Has(root)
}

func (a *Accessor) DeleteCar(root cid.Cid) error {
	return a.car.Delete(root)
}

func (a *Accessor) CountCar() (int, error) {
	return a.car.Count()
}

func (a *Accessor) PutWaitList(data []byte) error {
	return a.wl.Put(data)
}

func (a *Accessor) GetWaitList() ([]byte, error) {
	return a.wl.Get()
}
