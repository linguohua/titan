package storage

import (
	"context"
	"io"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-libipfs/blocks"
)

type Storage interface {
	PutAssetPuller(c cid.Cid, data []byte) error
	GetAssetPuller(c cid.Cid) ([]byte, error)
	HasAssetPuller(c cid.Cid) (bool, error)
	RemoveAssetPuller(c cid.Cid) error

	PutBlocks(ctx context.Context, root cid.Cid, blks []blocks.Block) error

	PutAsset(ctx context.Context, root cid.Cid) error
	GetAsset(root cid.Cid) (io.ReadSeekCloser, error)
	HasAsset(root cid.Cid) (bool, error)
	RemoveAsset(root cid.Cid) error
	CountAsset() (int, error)
	BlockCountOfAsset(ctx context.Context, root cid.Cid) (uint32, error)
	SetBlockCountOfAsset(ctx context.Context, root cid.Cid, count uint32) error

	// assets view
	GetTopHash(ctx context.Context) (string, error)
	GetBucketHashes(ctx context.Context) (map[uint32]string, error)
	GetAssetsOfBucket(ctx context.Context, bucketID uint32) ([]cid.Cid, error)
	AddAssetToAssetsView(ctx context.Context, root cid.Cid) error
	RemoveAssetFromAssetsView(ctx context.Context, root cid.Cid) error

	PutWaitList(data []byte) error
	GetWaitList() ([]byte, error)

	GetDiskUsageStat() (totalSpace, usage float64)
	GetFilesystemType() string
}
