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

// Car save car file
type car struct {
	baseDir string
	lru     *lruCache
}

func newCar(baseDir, carSuffix string, maxSizeOfCache int) (*car, error) {
	cache, err := newLRUCache(baseDir, maxSizeOfCache)
	if err != nil {
		return nil, err
	}
	return &car{baseDir: baseDir, lru: cache}, nil
}

func newCarName(root cid.Cid) string {
	return root.Hash().String() + carSuffix
}

func (c *car) putBlocks(ctx context.Context, root cid.Cid, blks []blocks.Block) error {
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

func (c *car) getBlock(ctx context.Context, root, block cid.Cid) (blocks.Block, error) {
	return c.lru.getBlock(ctx, root, block)
}

func (c *car) hasBlock(ctx context.Context, root, block cid.Cid) (bool, error) {
	return c.lru.hasBlock(ctx, root, block)
}

// CarReader must close reader
func (c *car) get(root cid.Cid) (io.ReadSeekCloser, error) {
	name := newCarName(root)
	filePath := filepath.Join(c.baseDir, name)

	return os.Open(filePath)
}

func (c *car) has(root cid.Cid) (bool, error) {
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

func (c *car) remove(root cid.Cid) error {
	// remove cache
	c.lru.remove(root)

	name := newCarName(root)
	path := filepath.Join(c.baseDir, name)

	// remove file
	return os.Remove(path)
}

func (c *car) count() (int, error) {
	entries, err := os.ReadDir(c.baseDir)
	if err != nil {
		return 0, err
	}

	return len(entries), nil
}
