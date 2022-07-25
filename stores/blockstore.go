package stores

import (
	"log"
	"os"
)

type BlockStore interface {
	Put(data []byte, cid string) error
	Get(cid string) ([]byte, error)
	Delete(cid string) error
}

func NewBlockStore(path string, storeType string) BlockStore {
	err := os.MkdirAll(path, 0755)
	if err != nil {
		log.Fatal(err)
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
