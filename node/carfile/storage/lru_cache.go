package storage

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"

	lru "github.com/hashicorp/golang-lru"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-libipfs/blocks"
	carv2 "github.com/ipld/go-car/v2"
	"github.com/ipld/go-car/v2/blockstore"
)

type Key string

// LRUCache lru cache
type lruCache struct {
	carsDir string
	cache   *lru.Cache
}

type cacheValue struct {
	bs     *blockstore.ReadOnly
	reader io.ReaderAt
}

func newLRUCache(carsDir string, maxSize int) (*lruCache, error) {
	b := &lruCache{carsDir: carsDir}
	cache, err := lru.NewWithEvict(maxSize, b.onEvict)
	if err != nil {
		return nil, err
	}
	b.cache = cache

	return b, nil
}

func (lc *lruCache) getBlock(ctx context.Context, root, block cid.Cid) (blocks.Block, error) {
	key := Key(root.Hash().String())
	if v, ok := lc.cache.Get(key); ok {
		if c, ok := v.(*cacheValue); ok {
			log.Debugf("get block %s from cache", block.String())
			return c.bs.Get(ctx, block)
		}
		return nil, fmt.Errorf("can not convert cache value to *blockstore.ReadOnly")
	}

	name := newCarName(root)
	path := filepath.Join(lc.carsDir, name)

	// close file on cache remove
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	bs, err := blockstore.NewReadOnly(file, nil, carv2.ZeroLengthSectionAsEOF(true))
	if err != nil {
		return nil, err
	}

	log.Debugf("add car %s index to cache", root.String())
	lc.cache.Add(Key(root.Hash().String()), &cacheValue{bs: bs, reader: file})

	return bs.Get(ctx, block)
}

func (lc *lruCache) hasBlock(ctx context.Context, root, block cid.Cid) (bool, error) {
	key := Key(root.Hash().String())
	if v, ok := lc.cache.Get(key); ok {
		if c, ok := v.(*cacheValue); ok {
			return c.bs.Has(ctx, block)
		}
		return false, fmt.Errorf("can not convert cache value to *blockstore.ReadOnly")
	}

	name := newCarName(root)
	path := filepath.Join(lc.carsDir, name)

	file, err := os.Open(path)
	if err != nil {
		return false, err
	}

	bs, err := blockstore.NewReadOnly(file, nil, carv2.ZeroLengthSectionAsEOF(true))
	if err != nil {
		return false, err
	}

	lc.cache.Add(Key(root.Hash().String()), &cacheValue{bs: bs, reader: file})

	return bs.Has(ctx, block)
}

func (lc *lruCache) remove(root cid.Cid) {
	lc.cache.Remove(Key(root.Hash().String()))
}

func (lc *lruCache) onEvict(key interface{}, value interface{}) {
	if c, ok := value.(*cacheValue); ok {
		c.bs.Close()
		if f, ok := c.reader.(*os.File); ok {
			f.Close()
		}
	}
}
