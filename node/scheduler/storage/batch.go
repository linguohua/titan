package storage

import (
	"context"
	"github.com/ipfs/go-datastore"
)

type op struct {
	delete bool
	value  []byte
}

type batch struct {
	ds  *Datastore
	ops map[datastore.Key]op
}

func (b *batch) Put(ctx context.Context, key datastore.Key, value []byte) error {
	b.ops[key] = op{value: value}
	return nil
}

func (b *batch) Delete(ctx context.Context, key datastore.Key) error {
	b.ops[key] = op{delete: true}
	return nil
}

func (b *batch) Commit(ctx context.Context) error {
	return nil
}

var _ datastore.Batch = (*batch)(nil)
