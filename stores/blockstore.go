package stores

import (
	"io"
	"log"
	"os"
)

type BlockStore interface {
	Put(data []byte, cid string) error
	Get(cid string) ([]byte, error)
	Delete(cid string) error
	GetReader(cid string) (BlockReader, error)
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

// func (br BlockReader) Read(p []byte) (int, error) {
// 	return br.Read(p)
// }

// func (br BlockReader) Close() error {
// 	return br.Close()
// }
