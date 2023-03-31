package storage

import (
	"context"
	"io"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-libipfs/blocks"
	"github.com/ipld/go-car/v2/index"
)

type Storage interface {
	PutCarCache(c cid.Cid, data []byte) error
	GetCarCache(c cid.Cid) ([]byte, error)
	HasCarCache(c cid.Cid) (bool, error)
	RemoveCarCache(c cid.Cid) error

	PutBlocks(ctx context.Context, root cid.Cid, blks []blocks.Block) error

	GetCar(root cid.Cid) (io.ReadSeekCloser, error)
	HasCar(root cid.Cid) (bool, error)
	FindCars(ctx context.Context, block cid.Cid) ([]cid.Cid, error)
	RemoveCar(root cid.Cid) error
	CountCar() (int, error)
	BlockCountOfCar(ctx context.Context, root cid.Cid) (uint32, error)
	SetBlockCountOfCar(ctx context.Context, root cid.Cid, count uint32) error

	// AddTopIndex mapped block cid to car cid
	// car must exist in local
	AddTopIndex(ctx context.Context, root cid.Cid, idx index.Index) error
	RemoveTopIndex(ctx context.Context, root cid.Cid, idx index.Index) error

	PutWaitList(data []byte) error
	GetWaitList() ([]byte, error)

	GetDiskUsageStat() (totalSpace, usage float64)
	GetFilesystemType() string
}
