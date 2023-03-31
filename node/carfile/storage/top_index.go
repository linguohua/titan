package storage

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	"github.com/ipld/go-car/v2/index"
	"github.com/multiformats/go-multihash"
	"golang.org/x/xerrors"
)

type topIndex struct {
	ds ds.Batching
}

func newTopIndex(baseDir string) (*topIndex, error) {
	ds, err := newKVstore(baseDir)
	if err != nil {
		return nil, err
	}

	return &topIndex{ds: ds}, nil
}

// Add index, the algorithm is o(n)
func (tIdx *topIndex) add(ctx context.Context, root cid.Cid, idx index.Index) error {
	iterableIdx, ok := idx.(index.IterableIndex)
	if !ok {
		return xerrors.Errorf("idx is not IterableIndex")
	}

	batch, err := tIdx.ds.Batch(ctx)
	if err != nil {
		return xerrors.Errorf("failed to create ds batch: %w", err)
	}

	if err = iterableIdx.ForEach(func(mh multihash.Multihash, u uint64) error {
		key := ds.NewKey(string(mh.String()))
		// do we already have an entry for this multihash ?
		val, err := tIdx.ds.Get(ctx, key)
		if err != nil && err != ds.ErrNotFound {
			return xerrors.Errorf("failed to get value for multihash %s, err: %w", mh, err)
		}

		// if we don't have an existing entry for this mh, create one
		if err == ds.ErrNotFound {
			mhs := []string{root.Hash().String()}
			bz, err := json.Marshal(mhs)
			if err != nil {
				return xerrors.Errorf("failed to marshal shard list to bytes: %w", err)
			}
			if err := batch.Put(ctx, key, bz); err != nil {
				return xerrors.Errorf("failed to put mh=%s, err=%w", mh, err)
			}
			return nil
		}

		// else , append the shard key to the existing list
		var mhs []string
		if err := json.Unmarshal(val, &mhs); err != nil {
			return fmt.Errorf("failed to unmarshal shard keys: %w", err)
		}

		// if we already have the shard key indexed for the multihash, nothing to do here.
		if has(mhs, root.Hash().String()) {
			return nil
		}

		mhs = append(mhs, root.Hash().String())
		bz, err := json.Marshal(mhs)
		if err != nil {
			return xerrors.Errorf("failed to marshal shard keys: %w", err)
		}
		if err := batch.Put(ctx, key, bz); err != nil {
			return xerrors.Errorf("failed to put mh=%s, err%w", mh, err)
		}

		return nil

	}); err != nil {
		return xerrors.Errorf("failed to add index entry: %w", err)
	}

	return batch.Commit(ctx)
}

// Remove index, the algorithm is o(n)
func (tIdx *topIndex) remove(ctx context.Context, root cid.Cid, idx index.Index) error {
	// TODO remove idx
	return nil
}

// find first car cid base on block cid
func (tIdx *topIndex) findCars(ctx context.Context, block cid.Cid) ([]cid.Cid, error) {
	key := ds.NewKey(block.Hash().String())
	v, err := tIdx.ds.Get(ctx, key)
	if err != nil && err != ds.ErrNotFound {
		return nil, err
	}

	if err == ds.ErrNotFound {
		return nil, nil
	}

	var cids []string
	err = json.Unmarshal(v, &cids)
	if err != nil {
		return nil, err
	}

	roots := make([]cid.Cid, 0, len(cids))
	for _, c := range cids {
		root, err := cid.Decode(c)
		if err != nil {
			return nil, err
		}

		roots = append(roots, root)
	}

	return roots, nil
}

func has(mhs []string, mh string) bool {
	for _, v := range mhs {
		if v == mh {
			return true
		}
	}
	return false
}
