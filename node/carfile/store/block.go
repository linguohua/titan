package store

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	lru "github.com/hashicorp/golang-lru"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-libipfs/blocks"
	"github.com/ipld/go-car/v2"
	"github.com/ipld/go-car/v2/blockstore"
	"github.com/ipld/go-car/v2/index"
)

const (
	maxSizeOfCache = 1024
)

type Block struct {
	carsDir string
	cache   *lru.Cache
}

func NewBlock(carsDir string) (*Block, error) {
	cache, err := lru.New(maxSizeOfCache)
	if err != nil {
		return nil, err
	}

	return &Block{carsDir: carsDir, cache: cache}, nil
}

func (b *Block) GetBlock(ctx context.Context, root, block cid.Cid) (blocks.Block, error) {
	key := Key(root.Hash().String())
	if v, ok := b.cache.Get(key); ok {
		if bs, ok := v.(*blockstore.ReadOnly); ok {
			return bs.Get(ctx, block)
		}
		return nil, fmt.Errorf("can not convert cache value to *blockstore.ReadOnly")
	}

	name := newCarName(root)
	path := filepath.Join(b.carsDir, name)

	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	bs, err := blockstore.NewReadOnly(file, b.idx, car.ZeroLengthSectionAsEOF(true))
	if err != nil {
		return nil, err
	}

	b.cache.Add(Key(root.Hash().String()), bs)
	// TODO: need to close file

	return bs.Get(ctx, block)
}

func (b *Block) HasBlock(ctx context.Context, root, block cid.Cid) (bool, error) {
	key := Key(root.Hash().String())
	if v, ok := b.cache.Get(key); ok {
		if bs, ok := v.(*blockstore.ReadOnly); ok {
			return bs.Has(ctx, block)
		}
		return false, fmt.Errorf("can not convert cache value to *blockstore.ReadOnly")
	}

	name := newCarName(root)
	path := filepath.Join(b.carsDir, name)

	file, err := os.Open(path)
	if err != nil {
		return false, err
	}

	bs, err := blockstore.NewReadOnly(file, b.idx, car.ZeroLengthSectionAsEOF(true))
	if err != nil {
		return false, err
	}

	b.cache.Add(Key(root.Hash().String()), bs)
	// TODO: need to close file

	return bs.Has(ctx, block)
}

func (b *Block) getIdx(carsDir string, root cid.Cid) (index.Index, error) {
	name := newCarName(root)
	path := filepath.Join(b.carsDir, name)

	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	defer f.Close()

	cr, err := car.NewReader(f)
	if err != nil {
		return nil, err
	}
	defer cr.Close()

	// Get root CIDs in the CARv1 file.
	roots, err := cr.Roots()
	if err != nil {
		panic(err)
	}

	// Read and unmarshall index within CARv2 file.
	ir, err := cr.IndexReader()
	if err != nil {
		panic(err)
	}
	idx, err := index.ReadFrom(ir)
	if err != nil {
		panic(err)
	}

	// For each root CID print the offset relative to CARv1 data payload.
	for _, r := range roots {
		offset, err := index.GetFirst(idx, r)
		if err != nil {
			panic(err)
		}
		fmt.Printf("Frame with CID %v starts at offset %v relative to CARv1 data payload.\n", r, offset)
	}
}
