package stores

import (
	"io"
	"log"
	"os"
)

type BlockStore interface {
	Put(key string, value []byte) error
	Get(key string) ([]byte, error)
	Delete(key string) error
	GetReader(key string) (BlockReader, error)
	Has(key string) (exists bool, err error)
	// GetSize(ctx context.Context, key string) (size int, err error)
}

func NewBlockStore(path string, storeType string) BlockStore {
	err := os.MkdirAll(path, 0755)
	if err != nil {
		log.Fatalf("NewBlockStore, path:%s, err:%v", path, err)
	}

	return NewBlockStoreFromString(storeType, path)
}

var RocksDB rocksdb
var FileStore fileStore

func NewBlockStoreFromString(t string, path string) BlockStore {
	switch t {
	case "RocksDB":
		RocksDB.Path = path
		return RocksDB
	case "FileStore":
		FileStore.Path = path
		return FileStore

	default:
		panic("unknown BlockStore type")
	}
}

type BlockReader interface {
	// file *os.File
	io.ReadCloser
	io.Seeker
}

// // Put implements Datastore.Put
// func (d *NullDatastore) Put(ctx context.Context, key Key, value []byte) (err error) {
// 	return nil
// }

// // Sync implements Datastore.Sync
// func (d *NullDatastore) Sync(ctx context.Context, prefix Key) error {
// 	return nil
// }

// // Get implements Datastore.Get
// func (d *NullDatastore) Get(ctx context.Context, key Key) (value []byte, err error) {
// 	return nil, ErrNotFound
// }

// // Has implements Datastore.Has
// func (d *NullDatastore) Has(ctx context.Context, key Key) (exists bool, err error) {
// 	return false, nil
// }

// // Has implements Datastore.GetSize
// func (d *NullDatastore) GetSize(ctx context.Context, key Key) (size int, err error) {
// 	return -1, ErrNotFound
// }

// // Delete implements Datastore.Delete
// func (d *NullDatastore) Delete(ctx context.Context, key Key) (err error) {
// 	return nil
// }

// // Query implements Datastore.Query
// func (d *NullDatastore) Query(ctx context.Context, q dsq.Query) (dsq.Results, error) {
// 	return dsq.ResultsWithEntries(q, nil), nil
// }

// func (d *NullDatastore) Batch(ctx context.Context) (Batch, error) {
// 	return NewBasicBatch(d), nil
// }

// func (d *NullDatastore) Close() error {
// 	return nil
// }
