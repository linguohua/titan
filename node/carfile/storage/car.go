package storage

import (
	"context"
	"io"
	"os"
	"path/filepath"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-libipfs/blocks"
	"github.com/ipld/go-car/v2/blockstore"
)

type Car struct {
	baseDir string
	lru     *LRUCache
}

func NewCar(baseDir, carSuffix string, maxSizeOfCache int) (*Car, error) {
	cache, err := NewLRUCache(baseDir, maxSizeOfCache)
	if err != nil {
		return nil, err
	}
	return &Car{baseDir: baseDir, lru: cache}, nil
}

func newCarName(root cid.Cid) string {
	return root.Hash().String() + carSuffix
}

func (c *Car) PutBlocks(ctx context.Context, root cid.Cid, blks []blocks.Block) error {
	name := newCarName(root)
	path := filepath.Join(c.baseDir, name)

	rw, err := blockstore.OpenReadWrite(path, []cid.Cid{root})
	if err != nil {
		return err
	}

	err = rw.PutMany(ctx, blks)
	if err != nil {
		return err
	}

	return rw.Finalize()
}

func (c *Car) GetBlock(ctx context.Context, root, block cid.Cid) (blocks.Block, error) {
	return c.lru.GetBlock(ctx, root, block)
}

func (c *Car) HasBlock(ctx context.Context, root, block cid.Cid) (bool, error) {
	return c.lru.HasBlock(ctx, root, block)
}

// CarReader must close reader
func (c *Car) Get(root cid.Cid) (io.ReadSeekCloser, error) {
	name := newCarName(root)
	filePath := filepath.Join(c.baseDir, name)

	return os.Open(filePath)
}

func (c *Car) Has(root cid.Cid) (bool, error) {
	name := newCarName(root)
	filePath := filepath.Join(c.baseDir, name)

	_, err := os.Stat(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}

		return false, err
	}

	return true, nil
}

func (c *Car) Delete(root cid.Cid) error {
	name := newCarName(root)
	path := filepath.Join(c.baseDir, name)

	return os.Remove(path)
}

func (c *Car) Count() (int, error) {
	entries, err := os.ReadDir(c.baseDir)
	if err != nil {
		return 0, err
	}

	return len(entries), nil
}
