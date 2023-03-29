package storage

import (
	"context"
	"io"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-libipfs/blocks"
)

type storage interface {
	PutCarCache(c cid.Cid, data []byte) error
	GetCarCache(c cid.Cid) ([]byte, error)
	HasCarCache(c cid.Cid) (bool, error)
	DeleteCarCache(c cid.Cid) error

	PutBlocks(ctx context.Context, root cid.Cid, blks []blocks.Block) error
	GetBlock(ctx context.Context, c cid.Cid) (blocks.Block, error)
	HasBlock(ctx context.Context, c cid.Cid) (bool, error)

	GetCar(root cid.Cid) (io.ReadSeekCloser, error)
	HasCar(root cid.Cid) (bool, error)
	DeleteCar(root cid.Cid) error
	CountCar() (int, error)

	PutWaitList(data []byte) error
	GetWaitList() ([]byte, error)
}
