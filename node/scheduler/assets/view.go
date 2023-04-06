package assets

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash/fnv"
	"sort"

	"github.com/ipfs/go-cid"
	"golang.org/x/xerrors"
)

func (m *Manager) RemoveAssetFromView(nodeID string, assetCID string) error {
	c, err := cid.Decode(assetCID)
	if err != nil {
		return err
	}

	bucketNumber := bucketNumber(c)
	bucketID := fmt.Sprintf("%s:%d", nodeID, bucketNumber)
	assetHashes, err := m.LoadBucket(bucketID)
	if err != nil {
		return xerrors.Errorf("load bucket error %w", err)
	}
	assetHashes = removeAssetHash(assetHashes, c.Hash().String())

	bucketHashes, err := m.LoadBucketHashes(nodeID)
	if err != nil {
		return err
	}

	if len(assetHashes) == 0 {
		if err := m.DeleteBucket(bucketID); err != nil {
			return err
		}
		delete(bucketHashes, bucketNumber)
	}

	if len(bucketHashes) == 0 {
		return m.DeleteAssetsView(nodeID)
	}

	topHash, err := calculateTopHash(bucketHashes)
	if err != nil {
		return err
	}

	if err := m.UpsertAssetsView(nodeID, topHash, bucketHashes); err != nil {
		return err
	}

	if len(assetHashes) > 0 {
		return m.UpsertBucket(bucketID, assetHashes)
	}
	return nil
}

func (m *Manager) AddAssetToView(nodeID string, assetCID string) error {
	c, err := cid.Decode(assetCID)
	if err != nil {
		return err
	}
	bucketNumber := bucketNumber(c)
	bucketID := fmt.Sprintf("%s:%d", nodeID, bucketNumber)
	assetHashes, err := m.LoadBucket(bucketID)
	if err != nil {
		return xerrors.Errorf("load bucket error %w", err)
	}

	if has(assetHashes, c.Hash().String()) {
		return nil
	}

	assetHashes = append(assetHashes, c.Hash().String())
	sort.Strings(assetHashes)

	hash, err := calculateBucketHash(assetHashes)
	if err != nil {
		return err
	}

	bucketHashes, err := m.LoadBucketHashes(nodeID)
	if err != nil {
		return err
	}
	bucketHashes[bucketNumber] = hash

	topHash, err := calculateTopHash(bucketHashes)
	if err != nil {
		return err
	}

	if err := m.UpsertAssetsView(nodeID, topHash, bucketHashes); err != nil {
		return err
	}

	return m.UpsertBucket(bucketID, assetHashes)
}

func bucketNumber(c cid.Cid) uint32 {
	h := fnv.New32a()
	if _, err := h.Write(c.Hash()); err != nil {
		log.Panicf("hash write buffer error %s", err.Error())
	}
	return h.Sum32() % sizeOfBuckets
}

func removeAssetHash(hashes []string, target string) []string {
	for i, hash := range hashes {
		if hash == target {
			return append(hashes[:i], hashes[i+1:]...)
		}
	}

	return hashes
}

func calculateBucketHash(hashes []string) (string, error) {
	hash := sha256.New()
	for _, h := range hashes {
		if cs, err := hex.DecodeString(h); err != nil {
			return "", err
		} else if _, err := hash.Write(cs); err != nil {
			return "", err
		}
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func calculateTopHash(hashes map[uint32]string) (string, error) {
	hash := sha256.New()
	for _, h := range hashes {
		if cs, err := hex.DecodeString(h); err != nil {
			return "", err
		} else if _, err := hash.Write(cs); err != nil {
			return "", err
		}
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func has(hashes []string, target string) bool {
	for _, hash := range hashes {
		if hash == target {
			return true
		}
	}
	return false
}
