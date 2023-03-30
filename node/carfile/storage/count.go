package storage

import (
	"context"
	"encoding/binary"

	"github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
)

type count struct {
	ds ds.Batching
}

func newCount(baseDir string) (*count, error) {
	ds, err := newKVstore(baseDir)
	if err != nil {
		return nil, err
	}

	return &count{ds: ds}, nil
}

func (c *count) put(ctx context.Context, root cid.Cid, count uint32) error {
	bs := make([]byte, 4)
	binary.LittleEndian.PutUint32(bs, count)
	return c.ds.Put(ctx, ds.NewKey(root.Hash().String()), bs)
}

func (c *count) get(ctx context.Context, root cid.Cid) (uint32, error) {
	val, err := c.ds.Get(ctx, ds.NewKey(root.Hash().String()))
	if err != nil {
		return 0, err
	}

	return binary.LittleEndian.Uint32(val), nil
}
