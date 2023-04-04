package storage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/gob"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/fnv"
	"sync"

	"github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	"github.com/multiformats/go-multihash"
	"golang.org/x/xerrors"
)

const (
	keyOfTopHash      = "top"
	keyOfBucketHashes = "checksums"
)

type AssetsView struct {
	*bucket

	lock *sync.Mutex
}

func newAssetsView(baseDir string, bucketSize uint32) (*AssetsView, error) {
	ds, err := newKVstore(baseDir)
	if err != nil {
		return nil, err
	}

	return &AssetsView{bucket: &bucket{ds: ds, size: bucketSize}, lock: &sync.Mutex{}}, nil
}

func (dv *AssetsView) setTopHash(ctx context.Context, checksum string) error {
	key := ds.NewKey(keyOfTopHash)
	return dv.ds.Put(ctx, key, []byte(checksum))
}

func (dv *AssetsView) getTopHash(ctx context.Context) (string, error) {
	key := ds.NewKey(keyOfTopHash)
	val, err := dv.ds.Get(ctx, key)
	if err != nil {
		return "", err
	}

	return string(val), nil
}

func (dv *AssetsView) setBucketHashes(ctx context.Context, checksums map[uint32]string) error {
	var buffer bytes.Buffer
	enc := gob.NewEncoder(&buffer)
	err := enc.Encode(checksums)
	if err != nil {
		return err
	}

	key := ds.NewKey(keyOfBucketHashes)
	return dv.ds.Put(ctx, key, buffer.Bytes())
}

func (dv *AssetsView) getBucketHashes(ctx context.Context) (map[uint32]string, error) {
	key := ds.NewKey(keyOfBucketHashes)
	val, err := dv.ds.Get(ctx, key)
	if err != nil {
		return nil, err
	}

	out := make(map[uint32]string)

	buffer := bytes.NewBuffer(val)
	dec := gob.NewDecoder(buffer)
	err = dec.Decode(&out)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (dv *AssetsView) addCar(ctx context.Context, root cid.Cid) error {
	dv.lock.Lock()
	defer dv.lock.Unlock()

	if err := dv.bucket.addCar(ctx, root); err != nil {
		return err
	}

	bucketID := dv.bucketID(root)
	if err := dv.updateHashes(ctx, bucketID); err != nil {
		return err
	}

	return nil
}

func (dv *AssetsView) removeCar(ctx context.Context, root cid.Cid) error {
	dv.lock.Lock()
	defer dv.lock.Unlock()

	if err := dv.bucket.removeCar(ctx, root); err != nil {
		return err
	}

	bucketID := dv.bucketID(root)
	if err := dv.updateHashes(ctx, bucketID); err != nil {
		return err
	}

	return nil
}

func (dv *AssetsView) updateHashes(ctx context.Context, bucketID uint32) error {
	cids, err := dv.getCars(ctx, bucketID)
	if err != nil {
		return err
	}

	hash, err := dv.calculateBucketHash(cids)
	if err != nil {
		return err
	}

	hashes, err := dv.getBucketHashes(ctx)
	if err != nil {
		return err
	}
	hashes[bucketID] = hash

	topHash, err := dv.calculateTopHash(hashes)
	if err != nil {
		return err
	}

	if err := dv.setBucketHashes(ctx, hashes); err != nil {
		return err
	}

	if err := dv.setTopHash(ctx, topHash); err != nil {
		return err
	}

	return nil
}

func (dv *AssetsView) calculateBucketHash(cids []cid.Cid) (string, error) {
	hash := sha256.New()
	for _, c := range cids {
		if _, err := hash.Write(c.Hash()); err != nil {
			return "", err
		}
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func (dv *AssetsView) calculateTopHash(checksums map[uint32]string) (string, error) {
	hash := sha256.New()
	for _, checksum := range checksums {
		if cs, err := hex.DecodeString(checksum); err != nil {
			return "", err
		} else if _, err := hash.Write(cs); err != nil {
			return "", err
		}
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

// bucket sort multi hash by hash code
type bucket struct {
	ds   ds.Batching
	size uint32
}

func (b *bucket) getCars(ctx context.Context, bucketID uint32) ([]cid.Cid, error) {
	if int(bucketID) > int(b.size) {
		return nil, fmt.Errorf("bucket index %d is out of %d", bucketID, b.size)
	}

	key := ds.NewKey(fmt.Sprintf("%d", bucketID))
	val, err := b.ds.Get(ctx, key)
	if err != nil && err != ds.ErrNotFound {
		return nil, xerrors.Errorf("failed to get value for bucket %d, err: %w", bucketID, err)
	}

	if errors.Is(err, ds.ErrNotFound) {
		return nil, nil
	}

	mhs, err := b.decode(val)
	if err != nil {
		return nil, err
	}

	cids := make([]cid.Cid, 0, len(mhs))
	for _, mh := range mhs {
		cids = append(cids, cid.NewCidV0(mh))
	}
	return cids, nil
}

func (b *bucket) addCar(ctx context.Context, root cid.Cid) error {
	bucketID := b.bucketID(root)
	key := ds.NewKey(fmt.Sprintf("%d", bucketID))

	val, err := b.ds.Get(ctx, key)
	if err != nil && err != ds.ErrNotFound {
		return xerrors.Errorf("failed to get value for bucket %d, err: %w", bucketID, err)
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

func (b *bucket) removeCar(ctx context.Context, root cid.Cid) error {
	bucketID := b.bucketID(root)
	key := ds.NewKey(fmt.Sprintf("%d", bucketID))

	val, err := b.ds.Get(ctx, key)
	if err != nil && err != ds.ErrNotFound {
		return xerrors.Errorf("failed to get value for bucket %d, err: %w", bucketID, err)
	}

	if errors.Is(err, ds.ErrNotFound) {
		return nil
	}

	mhs, err := b.decode(val)
	if err != nil {
		return xerrors.Errorf("decode bucket data: %w", err)
	}

	// remove mhs
	b.remove(mhs, root.Hash())

	if len(mhs) == 0 {
		return b.removeBucket(ctx, bucketID)
	}

	bs, err := b.encode(mhs)
	if err != nil {
		return xerrors.Errorf("marshal bucket data: %w", err)
	}

	return b.ds.Put(ctx, key, bs)
}

func (b *bucket) removeBucket(ctx context.Context, bucketID uint32) error {
	key := ds.NewKey(fmt.Sprintf("%d", bucketID))
	return b.ds.Delete(ctx, key)
}

func (b *bucket) bucketID(c cid.Cid) uint32 {
	h := fnv.New32a()
	h.Write(c.Hash())
	return h.Sum32() % b.size
}

func (b *bucket) has(mhs []multihash.Multihash, mh multihash.Multihash) bool {
	for _, v := range mhs {
		if bytes.Equal(v, mh) {
			return true
		}
	}

	return false
}

func (b *bucket) remove(sources []multihash.Multihash, target multihash.Multihash) []multihash.Multihash {
	// remove mhs
	for i, mh := range sources {
		if bytes.Equal(mh, target) {
			if i == 0 {
				sources = sources[1:]
			} else {
				sources = append(sources[:i], sources[i+1:]...)
			}
			return sources
		}
	}
	return sources
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
