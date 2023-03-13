package store

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/filecoin-project/dagstore"
	"github.com/filecoin-project/dagstore/index"
	"github.com/filecoin-project/dagstore/mount"
	"github.com/filecoin-project/dagstore/shard"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-libipfs/blocks"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipld/go-car/v2/blockstore"
	"github.com/multiformats/go-multihash"
)

var log = logging.Logger("/carfile/store")

const (
	// dir of file name
	incompleteCarfileCachesDir = "incomplete-carfiles"
	waitCacheListFile          = "wait-cache"
	carsDir                    = "cars"
	indexRepo                  = "full-index"
	topIndexDir                = "top-index"
	transientsDir              = "tmp"
	carSuffix                  = ".car"
)

func newCarfileName(root cid.Cid) string {
	return root.Hash().String() + carSuffix
}

type CarfileStore struct {
	incompleteCarfileCache *incompleteCarfileCache
	baseDir                string
	dagst                  *dagstore.DAGStore
	// manager full index
	indexRepo index.FullIndexRepo
}

func NewCarfileStore(path string) (*CarfileStore, error) {
	carsDirPath := filepath.Join(path, carsDir)
	err := os.MkdirAll(carsDirPath, 0o755)
	if err != nil {
		return nil, err
	}

	incompleteCarfileCachesPath := filepath.Join(path, incompleteCarfileCachesDir)
	incompleteCarfileCache := newIncompleteCarfileCache(incompleteCarfileCachesPath)

	opts := &dagstoreOpts{
		carsDir:       carsDirPath,
		indexRepo:     filepath.Join(path, indexRepo),
		topIndexDir:   filepath.Join(path, topIndexDir),
		transientsDir: filepath.Join(path, transientsDir),
	}

	dagstWrapper, err := newDagstore(opts)
	if err != nil {
		return nil, err
	}

	dagstWrapper.dagst.Start(context.Background())

	return &CarfileStore{incompleteCarfileCache: incompleteCarfileCache, baseDir: path, dagst: dagstWrapper.dagst, indexRepo: dagstWrapper.indexRepo}, nil
}

func (cs *CarfileStore) closeDagstore() error {
	return cs.dagst.Close()
}

func (cs *CarfileStore) NewCarfileWriter(root cid.Cid) (*blockstore.ReadWrite, error) {
	name := newCarfileName(root)
	path := filepath.Join(cs.carsDir(), name)

	return blockstore.OpenReadWrite(path, []cid.Cid{root})
}

func (cs *CarfileStore) HashCarfile(root cid.Cid) (bool, error) {
	name := newCarfileName(root)
	path := filepath.Join(cs.carsDir(), name)

	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}

	if os.IsNotExist(err) {
		return false, nil
	}

	return false, err
}

func (cs *CarfileStore) RegisterShared(root cid.Cid) error {
	name := newCarfileName(root)
	ch := make(chan dagstore.ShardResult)
	k := shard.KeyFromString(root.Hash().String())

	opts := dagstore.RegisterOpts{
		ExistingTransient: filepath.Join(cs.carsDir(), name),
	}

	err := cs.dagst.RegisterShard(context.Background(), k, &mount.FSMount{FS: os.DirFS(cs.carsDir()), Path: name}, ch, opts)
	if err != nil {
		return err
	}

	res := <-ch
	return res.Error
}

func (cs *CarfileStore) DeleteCarfile(root cid.Cid) error {
	err := cs.destroyShared(root)
	if err != nil {
		return err
	}

	name := newCarfileName(root)
	path := filepath.Join(cs.carsDir(), name)

	err = os.Remove(path)
	if err != nil && os.IsNotExist(err) {
		return datastore.ErrNotFound
	}
	return err
}

func (cs *CarfileStore) destroyShared(root cid.Cid) error {
	ch := make(chan dagstore.ShardResult, 0)
	k := shard.KeyFromString(root.Hash().String())

	err := cs.dagst.DestroyShard(context.Background(), k, ch, dagstore.DestroyOpts{})
	if err != nil {
		return err
	}

	res := <-ch
	if res.Error != nil {
		return res.Error
	}

	ok, err := cs.indexRepo.DropFullIndex(k)
	if err != nil {
		return err
	}

	if !ok {
		return fmt.Errorf("drop shard %s full index failed", k.String())
	}

	// TODO: continue drop topIndex
	return nil
}

func (cs *CarfileStore) carsDir() string {
	return filepath.Join(cs.baseDir, carsDir)
}

func (cs *CarfileStore) Block(c cid.Cid) (blocks.Block, error) {
	// shard in TopLevelIndex may be invalid, need to check it on dagstore
	ks, err := cs.dagst.TopLevelIndex.GetShardsForMultihash(context.Background(), c.Hash())
	if err != nil {
		return nil, err
	}

	if len(ks) == 0 {
		return nil, fmt.Errorf("block %s not exist", c.String())
	}

	var key shard.Key
	// find first valid shard
	for _, k := range ks {
		_, err := cs.dagst.GetShardInfo(k)
		if err == nil {
			key = k
			break
		}

		// ignore err
	}

	if len(key.String()) == 0 {
		return nil, fmt.Errorf("could not find a valid shard for block %s", c.String())
	}

	ch := make(chan dagstore.ShardResult)
	err = cs.dagst.AcquireShard(context.Background(), key, ch, dagstore.AcquireOpts{})
	if err != nil {
		return nil, err
	}

	res := <-ch
	if res.Error != nil {
		return nil, res.Error
	}

	if res.Accessor == nil {
		return nil, fmt.Errorf("can not get shard %s accessor", ks[0].String())
	}
	defer res.Accessor.Close()

	bs, err := res.Accessor.Blockstore()
	if err != nil {
		return nil, err
	}

	return bs.Get(context.Background(), c)
}

func (cs *CarfileStore) BlocksOfCarfile(root cid.Cid) ([]cid.Cid, error) {
	k := shard.KeyFromString(root.Hash().String())
	ii, err := cs.dagst.GetIterableIndex(k)
	if err != nil {
		return nil, err
	}

	cids := make([]cid.Cid, 0)
	err = ii.ForEach(func(m multihash.Multihash, u uint64) error {
		cid := cid.NewCidV1(cid.Raw, m)
		cids = append(cids, cid)
		return nil
	})

	if err != nil {
		return nil, err
	}

	return cids, nil

}

func (cs *CarfileStore) BlockReader(c cid.Cid) (io.ReadCloser, error) {
	return nil, fmt.Errorf("not implement")
}

func (cs *CarfileStore) indexCount(k shard.Key) (int, error) {
	ii, err := cs.dagst.GetIterableIndex(k)
	if err != nil {
		return 0, err
	}

	count := 0
	err = ii.ForEach(func(m multihash.Multihash, u uint64) error {
		count++
		return nil
	})

	if err != nil {
		return 0, err
	}

	return count, nil
}

// count all block is cost much performance
func (cs *CarfileStore) BlockCount() (int, error) {
	count := 0
	infos := cs.dagst.AllShardsInfo()
	for k := range infos {
		c, err := cs.indexCount(k)
		if err != nil {
			return 0, err
		}

		count += c
	}
	return count, nil
}

func (cs *CarfileStore) BlockCountOfCarfile(root cid.Cid) (int, error) {
	k := shard.KeyFromString(root.Hash().String())
	return cs.indexCount(k)
}

func (cs *CarfileStore) CarfileCount() (int, error) {
	infos := cs.dagst.AllShardsInfo()
	return len(infos), nil
}

func (cs *CarfileStore) BaseDir() string {
	return cs.baseDir
}

func (cs *CarfileStore) CarfileHashes() ([]string, error) {
	infos := cs.dagst.AllShardsInfo()
	multihashes := make([]string, 0, len(infos))

	for k := range infos {
		multihashes = append(multihashes, k.String())
	}
	return multihashes, nil
}

// incomplete carfileCache
func (cs *CarfileStore) SaveIncompleteCarfileCache(carfileHash string, carfileCacheData []byte) error {
	return cs.incompleteCarfileCache.save(carfileHash, carfileCacheData)
}

func (cs *CarfileStore) DeleteIncompleteCarfileCache(c cid.Cid) error {
	return cs.incompleteCarfileCache.delete(c.Hash().String())
}

// return datastore.ErrNotFound if car not exist
func (cs *CarfileStore) IncompleteCarfileCacheData(c cid.Cid) ([]byte, error) {
	return cs.incompleteCarfileCache.data(c.Hash().String())
}

func (cs *CarfileStore) IncompleteCarfileHashList() ([]string, error) {
	return cs.incompleteCarfileCache.carfileHashList()
}

// incomplete carfileCache
func (cs *CarfileStore) HasIncompleteCarfile(c cid.Cid) (bool, error) {
	return cs.incompleteCarfileCache.has(c.Hash().String())
}

// wait list file
func (cs *CarfileStore) SaveWaitList(data []byte) error {
	return saveWaitListToFile(data, filepath.Join(cs.baseDir, waitCacheListFile))
}

func (cs *CarfileStore) WaitList() ([]byte, error) {
	return getWaitListFromFile(filepath.Join(cs.baseDir, waitCacheListFile))
}

func saveWaitListToFile(data []byte, path string) error {
	return os.WriteFile(path, data, 0644)
}

func getWaitListFromFile(path string) ([]byte, error) {
	data, err := os.ReadFile(path)
	if err != nil && os.IsNotExist(err) {
		return nil, datastore.ErrNotFound
	}

	return data, err
}
