package stores

type rocksdb struct {
	Path string
}

func (r rocksdb) Type() string {
	return "RocksDB"
}

func (r rocksdb) Put(data []byte, cid string) error {
	return nil
}

func (r rocksdb) Get(cid string) ([]byte, error) {
	return nil, nil
}

func (r rocksdb) Delete(cid string) error {
	return nil
}

func (r rocksdb) GetReader(cid string) (BlockReader, error) {
	return nil, nil
}
