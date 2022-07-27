package stores

import "time"

var HeartbeatInterval = 10 * time.Second
var SkippedHeartbeatThresh = HeartbeatInterval * 5

// LocalStorageMeta [path]/sectorstore.json
type LocalStorageMeta struct {
	ID string

	GroupID string
	// how many sector instances can this storage allow
	MaxSealingSectors int

	// A high weight means data is more likely to be stored in this path
	Weight uint64 // 0 = readonly

	// Intermediate data for the sealing process will be stored here
	CanSeal bool

	// Finalized sectors that will be proved over time will be stored here
	CanStore bool

	// MaxStorage specifies the maximum number of bytes to use for sector storage
	// (0 = unlimited)
	MaxStorage uint64

	// List of storage groups this path belongs to
	Groups []string

	// List of storage groups to which data from this path can be moved. If none
	// are specified, allow to all
	AllowTo []string
}

// StorageConfig .lotusstorage/storage.json
type StorageConfig struct {
	StoragePaths []LocalPath
}

type LocalPath struct {
	Path string
}
