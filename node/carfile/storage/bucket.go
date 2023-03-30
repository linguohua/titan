package storage

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/fnv"

	"github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	"github.com/multiformats/go-multihash"
	"golang.org/x/xerrors"
)

// bucket sort multi hash by hash code
type bucket struct {
	ds   ds.Batching
	size int
}

func newBucket(baseDir string, bucketSize int) (*bucket, error) {
	ds, err := newKVstore(baseDir)
	if err != nil {
		return nil, err
	}

	return &bucket{ds: ds, size: bucketSize}, nil
}

func (b *bucket) put(ctx context.Context, root cid.Cid) error {
	index := b.index(root.Hash())
	key := ds.NewKey(fmt.Sprintf("%d", index))
	val, err := b.ds.Get(ctx, key)
	if err != nil && err != ds.ErrNotFound {
		return xerrors.Errorf("failed to get value for bucket %d, err: %w", index, err)
	}

	if errors.Is(err, ds.ErrNotFound) {
		mhs := []multihash.Multihash{root.Hash()}
		bs, err := b.encode(mhs)
		if err != nil {
			return xerrors.Errorf("encode bucket data: %w", err)
		}

		return b.ds.Put(ctx, key, bs)
	}

	mhs, err := b.decode(val)
	if err != nil {
		return xerrors.Errorf("decode bucket data: %w", err)
	}

	if b.has(mhs, root.Hash()) {
		return nil
	}

	mhs = append(mhs, root.Hash())
	bs, err := b.encode(mhs)
	if err != nil {
		return xerrors.Errorf("marshal bucket data: %w", err)
	}

	return b.ds.Put(ctx, key, bs)
}

func (b *bucket) get(ctx context.Context, index int) ([]multihash.Multihash, error) {
	if index > b.size {
		return nil, fmt.Errorf("bucket index %d is out of %d", index, b.size)
	}

	key := ds.NewKey(fmt.Sprintf("%d", index))
	val, err := b.ds.Get(ctx, key)
	if err != nil && err != ds.ErrNotFound {
		return nil, xerrors.Errorf("failed to get value for bucket %d, err: %w", index, err)
	}

	if errors.Is(err, ds.ErrNotFound) {
		return nil, nil
	}

	mhs, err := b.decode(val)
	if err != nil {
		return nil, err
	}
	return mhs, nil
}

func (b *bucket) index(mh multihash.Multihash) int {
	h := fnv.New32a()
	h.Write(mh)
	return int(h.Sum32()) % b.size
}

func (b *bucket) has(mhs []multihash.Multihash, mh multihash.Multihash) bool {
	for _, v := range mhs {
		if bytes.Equal(v, mh) {
			return true
		}
	}

	return false
}

func (b *bucket) encode(mhs []multihash.Multihash) ([]byte, error) {
	var buf bytes.Buffer
	for _, mh := range mhs {
		size := uint32(len(mh))
		err := binary.Write(&buf, binary.BigEndian, size)
		if err != nil {
			return nil, err
		}

		_, err = buf.Write(mh)
		if err != nil {
			return nil, err
		}
	}

	return buf.Bytes(), nil
}

func (b *bucket) decode(bs []byte) ([]multihash.Multihash, error) {
	sizeOfUint32 := 4
	mhs := make([]multihash.Multihash, 0)
	for len(bs) > 0 {
		if len(bs) < sizeOfUint32 {
			return nil, xerrors.Errorf("can not get multi hash size")
		}

		size := binary.BigEndian.Uint32(bs[:sizeOfUint32])
		if int(size) > len(bs)-sizeOfUint32 {
			return nil, xerrors.Errorf("multi hash size if out of range")
		}

		bs = bs[sizeOfUint32:]
		mhs = append(mhs, bs[:size])
		bs = bs[size:]
	}

	return mhs, nil
}
