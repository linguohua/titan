package api

import (
	"context"
)

// DataSync sync scheduler asset to node
type DataSync interface {
	// CompareTopChecksums can check asset is same as scheduler
	// topChecksum is checksum of all buckets
	CompareTopChecksum(ctx context.Context, topChecksum string) (bool, error) //perm:write
	// CompareBucketChecksums group asset in bucket, and compare single bucket checksum
	// checksums are map of bucket, key is bucket index, value is checksum
	// return mismatch bucket index
	CompareBucketChecksums(ctx context.Context, checksums map[uint32]string) ([]uint32, error) //perm:write
}
