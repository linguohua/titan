package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	levelds "github.com/ipfs/go-ds-leveldb"
	"github.com/ipld/go-car/v2/index"
	"github.com/multiformats/go-multihash"
	ldbopts "github.com/syndtr/goleveldb/leveldb/opt"
	"golang.org/x/xerrors"
)

type TopIndex struct {
	ds ds.Batching
}

func NewTopIndex(baseDir string) (*TopIndex, error) {
	ds, err := newDatastore(baseDir)
	if err != nil {
		return nil, err
	}

	return &TopIndex{ds: ds}, nil
}

func newDatastore(baseDir string) (ds.Batching, error) {
	// Create the datastore directory if it doesn't exist yet.
	if err := os.MkdirAll(baseDir, 0755); err != nil {
		return nil, xerrors.Errorf("failed to create directory %s for DAG store datastore: %w", baseDir, err)
	}

	// Create a new LevelDB datastore
	return levelds.NewDatastore(baseDir, &levelds.Options{
		Compression: ldbopts.NoCompression,
		NoSync:      false,
		Strict:      ldbopts.StrictAll,
		ReadOnly:    false,
	})
}

// Add index
func (tIdx *TopIndex) Add(ctx context.Context, root cid.Cid, idx index.Index) error {
	iterableIdx, ok := idx.(index.IterableIndex)
	if !ok {
		return xerrors.Errorf("idx is not IterableIndex")
	}

	batch, err := tIdx.ds.Batch(ctx)
	if err != nil {
		return xerrors.Errorf("failed to create ds batch: %w", err)
	}

	if err = iterableIdx.ForEach(func(mh multihash.Multihash, u uint64) error {
		key := ds.NewKey(string(mh))
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

// Remove index
func (tIdx *TopIndex) Remove(idx index.Index) error {
	return nil
}

// find first car cid base on block cid
func (tIdx *TopIndex) FindCar(ctx context.Context, block cid.Cid) (*cid.Cid, error) {
	key := ds.NewKey(block.Hash().String())
	v, err := tIdx.ds.Get(ctx, key)
	if err != nil {
		return nil, err
	}

	var cids []string
	err = json.Unmarshal(v, &cids)
	if err != nil {
		return nil, err
	}

	if len(cids) == 0 {
		return nil, xerrors.Errorf("not exist car for block %s", block.String())
	}

	c, err := cid.Decode(cids[0])
	if err != nil {
		return nil, err
	}
	return &c, nil
}

func has(mhs []string, mh string) bool {
	for _, v := range mhs {
		if v == mh {
			return true
		}
	}
	return false
}
