package carfilestore

import (
	"context"
	"path"

	ldbopts "github.com/syndtr/goleveldb/leveldb/opt"

	"github.com/ipfs/go-datastore"
	levelds "github.com/ipfs/go-ds-leveldb"
)

const kvstorePath = "/link/"

type linksStore struct {
	path string
	ds   datastore.Batching
}

func newLinksStore(path string) *linksStore {
	ds, err := levelds.NewDatastore(path, &levelds.Options{
		Compression: ldbopts.NoCompression,
		NoSync:      false,
		Strict:      ldbopts.StrictAll,
		ReadOnly:    false,
	})

	if err != nil {
		log.Panicf("newKVStore error:%s, path:%s", err.Error(), path)
	}
	return &linksStore{path: path, ds: ds}
}

func (ls *linksStore) get(ctx context.Context, blockHash string) ([]byte, error) {
	linkPath := path.Join(kvstorePath, blockHash)
	return ls.ds.Get(ctx, datastore.NewKey(linkPath))
}

func (ls *linksStore) put(ctx context.Context, blockHash string, links []byte) error {
	linkPath := path.Join(kvstorePath, blockHash)
	return ls.ds.Put(context.Background(), datastore.NewKey(linkPath), links)
}

func (ls *linksStore) delete(ctx context.Context, blockHash string) error {
	linkPath := path.Join(kvstorePath, blockHash)
	return ls.ds.Delete(context.Background(), datastore.NewKey(linkPath))
}
