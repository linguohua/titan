package gateway

import (
	"context"
	"fmt"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-libipfs/blocks"
)

// BlockService interface implement
type readOnlyBlockStore struct {
	gw *Gateway
}

func (robs *readOnlyBlockStore) DeleteBlock(context.Context, cid.Cid) error {
	return fmt.Errorf("read only block store, can not delete block")
}

func (robs *readOnlyBlockStore) Has(ctx context.Context, c cid.Cid) (bool, error) {
	return robs.gw.storage.HasBlock(ctx, c)
}

func (robs *readOnlyBlockStore) Get(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	return robs.gw.storage.GetBlock(ctx, c)
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
