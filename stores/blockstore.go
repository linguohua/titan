package stores

import (
	"io"
	"os"

	logging "github.com/ipfs/go-log/v2"
	"github.com/linguohua/titan/node/fsutil"
)

var log = logging.Logger("stores")

type BlockStore interface {
	Put(key string, value []byte) error
	Get(key string) ([]byte, error)
	Delete(key string) error
	GetReader(key string) (BlockReader, error)
	Has(key string) (exists bool, err error)
	Stat() (fsutil.FsStat, error)
	KeyCount() (int, error)
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
		return &RocksDB
	case "FileStore":
		FileStore.Path = path
		return &FileStore

	default:
		panic("unknown BlockStore type")
	}
}

type BlockReader interface {
	io.ReadCloser
	io.Seeker

	Size() int64
}
