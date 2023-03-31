package cache

import (
	"context"
	"io"
	"os"

	lru "github.com/hashicorp/golang-lru"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-libipfs/blocks"
	carv2 "github.com/ipld/go-car/v2"
	"github.com/ipld/go-car/v2/blockstore"
	"github.com/ipld/go-car/v2/index"
	"github.com/linguohua/titan/node/carfile/storage"
	"golang.org/x/xerrors"
)

type Key string

// lruCache car index cache
type lruCache struct {
	storage storage.Storage
	cache   *lru.Cache
}

type cacheValue struct {
	bs          *blockstore.ReadOnly
	readerClose io.ReadCloser
	idx         index.Index
}

func newLRUCache(storage storage.Storage, maxSize int) (*lruCache, error) {
	b := &lruCache{storage: storage}
	cache, err := lru.NewWithEvict(maxSize, b.onEvict)
	if err != nil {
		return nil, err
	}
	b.cache = cache

	return b, nil
}

func (lru *lruCache) getBlock(ctx context.Context, root, block cid.Cid) (blocks.Block, error) {
	key := Key(root.Hash().String())
	v, ok := lru.cache.Get(key)
	if !ok {
		if err := lru.add(root); err != nil {
			return nil, err
		}

		log.Debugf("get block %s from cache", block.String())

		if v, ok = lru.cache.Get(key); !ok {
			return nil, xerrors.Errorf("car %s not exist", root.String())
		}
	}

	if c, ok := v.(*cacheValue); ok {
		return c.bs.Get(ctx, block)
	}

	return nil, xerrors.Errorf("can not convert interface to *cacheValue")
}

func (lru *lruCache) hasBlock(ctx context.Context, root, block cid.Cid) (bool, error) {
	key := Key(root.Hash().String())
	v, ok := lru.cache.Get(key)
	if !ok {
		if err := lru.add(root); err != nil {
			return false, err
		}

		log.Debugf("check car %s index from cache", block.String())

		if v, ok = lru.cache.Get(key); !ok {
			return false, xerrors.Errorf("car %s not exist", root.String())
		}
	}

	if c, ok := v.(*cacheValue); ok {
		return c.bs.Has(ctx, block)
	}

	return false, xerrors.Errorf("can not convert interface to *cacheValue")
}

func (lru *lruCache) carIndex(root cid.Cid) (index.Index, error) {
	key := Key(root.Hash().String())
	v, ok := lru.cache.Get(key)
	if !ok {
		if err := lru.add(root); err != nil {
			return nil, err
		}

		if v, ok = lru.cache.Get(key); !ok {
			return nil, xerrors.Errorf("car %s not exist", root.String())
		}
	}

	if c, ok := v.(*cacheValue); ok {
		return c.idx, nil
	}

	return nil, xerrors.Errorf("can not convert interface to *cacheValue")
}

func (lru *lruCache) add(root cid.Cid) error {
	reader, err := lru.storage.GetCar(root)
	if err != nil {
		return err
	}

	f, ok := reader.(*os.File)
	if !ok {
		return xerrors.Errorf("can not convert car %s reader to file", root.String())
	}

	idx, err := lru.getCarIndex(f)
	if err != nil {
		return err
	}

	bs, err := blockstore.NewReadOnly(f, idx, carv2.ZeroLengthSectionAsEOF(true))
	if err != nil {
		return err
	}

	cache := &cacheValue{bs: bs, readerClose: f, idx: idx}
	lru.cache.Add(Key(root.Hash().String()), cache)

	return nil
}

func (lru *lruCache) remove(root cid.Cid) {
	lru.cache.Remove(Key(root.Hash().String()))
}

func (lru *lruCache) onEvict(key interface{}, value interface{}) {
	if c, ok := value.(*cacheValue); ok {
		c.bs.Close()
		c.readerClose.Close()
	}
}

func (lru *lruCache) getCarIndex(r io.ReaderAt) (index.Index, error) {
	// Open the CARv2 file
	cr, err := carv2.NewReader(r)
	if err != nil {
		panic(err)
	}
	defer cr.Close()

	// Read and unmarshall index within CARv2 file.
	ir, err := cr.IndexReader()
	if err != nil {
		return nil, err
	}
	idx, err := index.ReadFrom(ir)
	if err != nil {
		return nil, err
	}
	return idx, nil
}
