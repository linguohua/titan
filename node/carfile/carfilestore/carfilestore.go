package carfilestore

import (
	"context"
	"path/filepath"

	logging "github.com/ipfs/go-log/v2"
	"github.com/linguohua/titan/blockstore"
	"github.com/linguohua/titan/node/fsutil"
)

var log = logging.Logger("carfilestore")

const (
	blocksDir                  = "blocks"
	carfileTableDir            = "carfiles"
	incompleteCarfileCachesDir = "incomplete-carfiles"
	waitCacheListFile          = "wait-cache"
	linksDir                   = "links"
)

type CarfileStore struct {
	blockStore             blockstore.BlockStore
	carfileTable           *carfileTable
	incompleteCarfileCache *incompleteCarfileCache
	// save relation of block belong to which storage
	linksStore *linksStore
	path       string
}

func NewCarfileStore(path, blockStoreType string) *CarfileStore {
	blocksPath := filepath.Join(path, blocksDir)
	blockStore := blockstore.NewBlockStore(blocksPath, blockStoreType)

	carfileTablePath := filepath.Join(path, carfileTableDir)
	carfileTable := newCarfileTable(carfileTablePath)

	incompleteCarfileCachesPath := filepath.Join(path, incompleteCarfileCachesDir)
	incompleteCarfileCache := newIncompleteCarfileCache(incompleteCarfileCachesPath)

	linksStorePath := filepath.Join(path, linksDir)
	linksStore := newLinksStore(linksStorePath)

	return &CarfileStore{
		blockStore:             blockStore,
		carfileTable:           carfileTable,
		incompleteCarfileCache: incompleteCarfileCache, path: path,
		linksStore: linksStore,
	}
}

// blockstore
func (carfileStore *CarfileStore) SaveBlock(blockHash string, blockData []byte) error {
	return carfileStore.blockStore.Put(blockHash, blockData)
}

func (carfileStore *CarfileStore) Block(blockHash string) ([]byte, error) {
	return carfileStore.blockStore.Get(blockHash)
}

func (carfileStore *CarfileStore) DeleteBlock(blockHash string) error {
	return carfileStore.blockStore.Delete(blockHash)
}

func (carfileStore *CarfileStore) BlockReader(blockHash string) (blockstore.BlockReader, error) {
	return carfileStore.blockStore.GetReader(blockHash)
}

func (carfileStore *CarfileStore) HasBlock(blockHash string) (exists bool, err error) {
	return carfileStore.blockStore.Has(blockHash)
}

func (carfileStore *CarfileStore) Stat() (fsutil.FsStat, error) {
	return carfileStore.blockStore.Stat()
}

func (carfileStore *CarfileStore) BlocksHashes() ([]string, error) {
	return carfileStore.blockStore.GetAllKeys()
}

func (carfileStore *CarfileStore) BlockCount() (int, error) {
	return carfileStore.blockStore.KeyCount()
}

// storage table
func (carfileStore *CarfileStore) CarfileCount() (int, error) {
	count1, err := carfileStore.carfileTable.carfileCount()
	if err != nil {
		return 0, err
	}

	count2, err := carfileStore.incompleteCarfileCache.carfileCount()
	if err != nil {
		return 0, err
	}

	return count1 + count2, nil
}

func (carfileStore *CarfileStore) Path() string {
	return carfileStore.path
}

func (carfileStore *CarfileStore) SaveBlockListOfCarfile(carfileHash string, blocksHashString string) error {
	return carfileStore.carfileTable.saveBlocksHashes(carfileHash, blocksHashString)
}

func (carfileStore *CarfileStore) DeleteCarfileTable(carfileHash string) error {
	return carfileStore.carfileTable.delete(carfileHash)
}

func (carfileStore *CarfileStore) BlocksHashesWith(carfileHash string, positions []int) ([]string, error) {
	return carfileStore.carfileTable.readBlocksHashesWith(carfileHash, positions)
}

func (carfileStore *CarfileStore) BlocksHashesOfCarfile(carfileHash string) ([]string, error) {
	return carfileStore.carfileTable.readBlocksHashesOfCarfile(carfileHash)
}

func (carfileStore *CarfileStore) HasCarfile(carfileHash string) (bool, error) {
	return carfileStore.carfileTable.has(carfileHash)
}

func (carfileStore *CarfileStore) BlockCountOfCarfile(carfileHash string) (int, error) {
	return carfileStore.carfileTable.blockCountOfCarfile(carfileHash)
}

func (carfileStore *CarfileStore) CompleteCarfileHashList() ([]string, error) {
	return carfileStore.carfileTable.carfileHashList()
}

// incomplete carfileCache
func (carfileStore *CarfileStore) SaveIncompleteCarfileCache(carfileHash string, carfileCacheData []byte) error {
	return carfileStore.incompleteCarfileCache.save(carfileHash, carfileCacheData)
}

func (carfileStore *CarfileStore) DeleteIncompleteCarfileCache(carfileHash string) error {
	return carfileStore.incompleteCarfileCache.delete(carfileHash)
}

func (carfileStore *CarfileStore) IncompleteCarfileCacheData(carfileHash string) ([]byte, error) {
	return carfileStore.incompleteCarfileCache.data(carfileHash)
}

func (carfileStore *CarfileStore) IncompleteCarfileHashList() ([]string, error) {
	return carfileStore.incompleteCarfileCache.carfileHashList()
}

// wait list file
func (carfileStore *CarfileStore) SaveWaitList(data []byte) error {
	return saveWaitListToFile(data, filepath.Join(carfileStore.path, waitCacheListFile))
}

func (carfileStore *CarfileStore) WaitList() ([]byte, error) {
	return getWaitListFromFile(filepath.Join(carfileStore.path, waitCacheListFile))
}

// save block link to carfiles
func (carfileStore *CarfileStore) SaveLinks(ctx context.Context, blockHash string, links []byte) error {
	return carfileStore.linksStore.put(ctx, blockHash, links)
}

// block links to carfiles
func (carfileStore *CarfileStore) Links(ctx context.Context, blockHash string) ([]byte, error) {
	return carfileStore.linksStore.get(ctx, blockHash)
}

func (carfileStore *CarfileStore) DeleteLinks(ctx context.Context, blockHash string) error {
	return carfileStore.linksStore.delete(ctx, blockHash)
}
