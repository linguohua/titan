package blockstore

import (
	"github.com/linguohua/titan/node/fsutil"
)

type rocksdb struct {
	Path string
}

func (r *rocksdb) Type() string {
	return "RocksDB"
}

func (r *rocksdb) Put(key string, value []byte) error {
	return nil
}

func (r *rocksdb) Get(key string) ([]byte, error) {
	return nil, nil
}

func (r *rocksdb) Delete(key string) error {
	return nil
}

func (r *rocksdb) GetReader(key string) (BlockReader, error) {
	return nil, nil
}

func (r *rocksdb) Has(key string) (exists bool, err error) {
	return false, err
}

func (r *rocksdb) GetSize(key string) (size int, err error) {
	return 0, nil
}

func (r *rocksdb) Stat() (fsutil.FsStat, error) {
	return fsutil.FsStat{}, nil
}

func (r *rocksdb) DiskUsage() (int64, error) {
	return 0, nil
}

func (r *rocksdb) KeyCount() (int, error) {
	return 0, nil
}

func (r *rocksdb) GetAllKeys() ([]string, error) {
	return []string{}, nil
}
