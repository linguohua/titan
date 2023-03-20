package gateway

import (
	"context"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-libipfs/blocks"
)

// BlockService interface implement
type readOnlyBlockStore struct {
	gw *Gateway
}

func (robs *readOnlyBlockStore) DeleteBlock(context.Context, cid.Cid) error {
	log.Errorf("read only block store, can not delete block")
	return nil
}

func (robs *readOnlyBlockStore) Has(ctx context.Context, c cid.Cid) (bool, error) {
	return robs.gw.carStore.HasBlock(c)
}

func (robs *readOnlyBlockStore) Get(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	return robs.gw.block(ctx, c)
}

// GetSize returns the CIDs mapped BlockSize
func (robs *readOnlyBlockStore) GetSize(ctx context.Context, c cid.Cid) (int, error) {
	log.Errorf("read only block store, can not get size %s", c.String())
	return 0, nil
}

// Put puts a given block to the underlying datastore
func (robs *readOnlyBlockStore) Put(context.Context, blocks.Block) error {
	log.Errorf("read only block store, can not put")
	return nil
}

func (robs *readOnlyBlockStore) PutMany(context.Context, []blocks.Block) error {
	log.Errorf("read only block store, can not put many")
	return nil
}

func (robs *readOnlyBlockStore) AllKeysChan(ctx context.Context) (<-chan cid.Cid, error) {
	log.Errorf("read only block store, not support allkeys ")
	return nil, nil
}

func (robs *readOnlyBlockStore) HashOnRead(enabled bool) {

}
