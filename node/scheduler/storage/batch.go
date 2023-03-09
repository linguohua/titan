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
	return b.CommitContext(ctx)
}

func (b *batch) CommitContext(ctx context.Context) error {
	conn, err := b.ds.DB.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()

	for k, op := range b.ops {
		_ = k
		if op.delete {
			//_, err = conn.ExecContext(ctx, b.ds.DeleteCarfileSql(), k.String())
		} else {
			//_, err = conn.ExecContext(ctx, b.ds.UpdateOrCreateCarfileSql(), k.String(), op.value)
		}
		if err != nil {
			break
		}
	}

	return err
}

var _ datastore.Batch = (*batch)(nil)
