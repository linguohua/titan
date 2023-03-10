package api

import (
	"context"
)

type DataSync interface {
	// sort multiHashes by bucketCount
	// checksums's key is hash code, within 0 ~ bucketCount
	// return key of mismatch checksum
	CompareChecksums(ctx context.Context, bucketCount uint32, checksums map[uint32]string) (mismatchKeys []uint32, err error) //perm:write
	// compare carfile one by one
	// multiHashes's key is hash code, within 0 ~ bucketCount
	CompareCarfiles(ctx context.Context, bucketCount uint32, multiHashes map[uint32][]string) error //perm:write
}
