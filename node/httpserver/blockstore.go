package httpserver

import (
	"context"
	"fmt"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
)

// BlockService interface implement
type readOnlyBlockStore struct {
	hs   *HttpServer
	root cid.Cid
}

func (robs *readOnlyBlockStore) DeleteBlock(context.Context, cid.Cid) error {
	return fmt.Errorf("read only block store, can not delete block")
}

func (robs *readOnlyBlockStore) Has(ctx context.Context, c cid.Cid) (bool, error) {
	return robs.hs.asset.HasBlock(ctx, robs.root, c)
}

func (robs *readOnlyBlockStore) Get(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	return robs.hs.asset.GetBlock(ctx, robs.root, c)
}

// GetSize returns the CIDs mapped BlockSize
func (robs *readOnlyBlockStore) GetSize(ctx context.Context, c cid.Cid) (int, error) {
	log.Errorf("read only block store, can not get size %s", c.String())
	return 0, nil
}

// Put puts a given block to the underlying datastore
func (robs *readOnlyBlockStore) Put(context.Context, blocks.Block) error {
	return fmt.Errorf("read only block store, can not put")
}

func (robs *readOnlyBlockStore) PutMany(context.Context, []blocks.Block) error {
	return fmt.Errorf("read only block store, can not put many")
}

func (robs *readOnlyBlockStore) AllKeysChan(ctx context.Context) (<-chan cid.Cid, error) {
	return nil, fmt.Errorf("read only block store, not support all keys")
}

func (robs *readOnlyBlockStore) HashOnRead(enabled bool) {

}
