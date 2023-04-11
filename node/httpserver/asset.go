package httpserver

import (
	"context"
	"io"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-libipfs/blocks"
)

type Asset interface {
	GetAsset(root cid.Cid) (io.ReadSeekCloser, error)
	AssetExists(root cid.Cid) (bool, error)

	HasBlock(ctx context.Context, root, block cid.Cid) (bool, error)
	GetBlock(ctx context.Context, root, block cid.Cid) (blocks.Block, error)
}
