package storage

import (
	"context"
	"io"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-libipfs/blocks"
)

type Storage interface {
	PutCarCache(c cid.Cid, data []byte) error
	GetCarCache(c cid.Cid) ([]byte, error)
	HasCarCache(c cid.Cid) (bool, error)
	RemoveCarCache(c cid.Cid) error

	PutBlocks(ctx context.Context, root cid.Cid, blks []blocks.Block) error

	PutCar(ctx context.Context, root cid.Cid) error
	GetCar(root cid.Cid) (io.ReadSeekCloser, error)
	HasCar(root cid.Cid) (bool, error)
	RemoveCar(root cid.Cid) error
	CountCar() (int, error)
	BlockCountOfCar(ctx context.Context, root cid.Cid) (uint32, error)
	SetBlockCountOfCar(ctx context.Context, root cid.Cid, count uint32) error

	// assets view
	GetTopHash(ctx context.Context) (string, error)
	GetBucketHashes(ctx context.Context) (map[uint32]string, error)
	GetCarsOfBucket(ctx context.Context, bucketID uint32) ([]cid.Cid, error)
	AddCarToAssetsView(ctx context.Context, root cid.Cid) error
	RemoveCarFromAssetsView(ctx context.Context, root cid.Cid) error

	PutWaitList(data []byte) error
	GetWaitList() ([]byte, error)

	GetDiskUsageStat() (totalSpace, usage float64)
	GetFilesystemType() string
}