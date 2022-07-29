//go:build !windows
// +build !windows

package stores

import (
	"bytes"
	"errors"
	"log"

	"github.com/tecbot/gorocksdb"
)

var (
	db           *gorocksdb.DB
	writeOptions *gorocksdb.WriteOptions
	readOptions  *gorocksdb.ReadOptions
)

func newOptions() {
	writeOptions = gorocksdb.NewDefaultWriteOptions()
	writeOptions.SetSync(true)

	readOptions = gorocksdb.NewDefaultReadOptions()
	readOptions.SetFillCache(false)

}

func openRocksDB(path string) (*gorocksdb.DB, error) {
	bbto := gorocksdb.NewDefaultBlockBasedTableOptions()
	bbto.SetBlockCache(gorocksdb.NewLRUCache(64 * 1024))
	opts := gorocksdb.NewDefaultOptions()
	opts.SetBlockBasedTableFactory(bbto)
	// opts.setco
	opts.SetCreateIfMissing(true)
	opts.SetMaxOpenFiles(5000)
	opts.SetInfoLogLevel(gorocksdb.DebugInfoLogLevel)

	db, err := gorocksdb.OpenDb(opts, path)

	if err != nil {
		log.Fatalln("OPEN DB error", db, err)
		db.Close()
		return nil, errors.New("fail to open db")
	} else {
		log.Println("OPEN DB success", db)
	}
	return db, nil
}

func getRocksDB(path string) (*gorocksdb.DB, error) {
	if db == nil {
		rdb, err := openRocksDB(path)
		if err != nil {
			log.Fatal("Open rocks db failed:", err)
			return nil, err
		}

		db = rdb

		newOptions()
	}

	return db, nil
}

type rocksdb struct {
	Path string
}

func (r rocksdb) Type() string {
	return "RocksDB"
}

func (r rocksdb) Put(data []byte, cid string) error {
	rdb, err := getRocksDB(r.Path)
	if err != nil {
		log.Fatal("Get rocks db failed:", err)
		return err
	}

	var key = []byte(cid)
	return rdb.Put(writeOptions, key, data)
}

func (r rocksdb) Get(cid string) ([]byte, error) {
	rdb, err := getRocksDB(r.Path)
	if err != nil {
		log.Fatal("Get rocks db failed:", err)
		return nil, err
	}

	var key = []byte(cid)
	slice, err := rdb.Get(readOptions, key)
	if err != nil {
		return nil, err
	}

	defer slice.Free()

	return slice.Data(), nil
}

func (r rocksdb) Delete(cid string) error {
	rdb, err := getRocksDB(r.Path)
	if err != nil {
		log.Fatal("Get rocks db failed:", err)
		return err
	}

	var key = []byte(cid)
	return rdb.Delete(writeOptions, key)
}

func (r rocksdb) GetReader(cid string) (BlockReader, error) {
	rdb, err := getRocksDB(r.Path)
	if err != nil {
		log.Fatal("Get rocks db failed:", err)
		return nil, err
	}

	var key = []byte(cid)
	slice, err := rdb.Get(readOptions, key)
	if err != nil {
		return nil, err
	}

	defer slice.Free()

	reader := bytes.NewReader(slice.Data())
	return &Reader{reader}, nil
}

type Reader struct {
	r *bytes.Reader
}

func (r Reader) Read(p []byte) (n int, err error) {
	return r.r.Read(p)
}

func (r Reader) Close() error {
	return nil
}

func (r *Reader) Seek(offset int64, whence int) (int64, error) {
	return r.r.Seek(offset, whence)
}
