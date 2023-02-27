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
	// save relation of block belong to which carfile
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

func (carfileStore *CarfileStore) GetBlock(blockHash string) ([]byte, error) {
	return carfileStore.blockStore.Get(blockHash)
}

func (carfileStore *CarfileStore) DeleteBlock(blockHash string) error {
	return carfileStore.blockStore.Delete(blockHash)
}

func (carfileStore *CarfileStore) GetBlockReader(blockHash string) (blockstore.BlockReader, error) {
	return carfileStore.blockStore.GetReader(blockHash)
}

func (carfileStore *CarfileStore) HasBlock(blockHash string) (exists bool, err error) {
	return carfileStore.blockStore.Has(blockHash)
}

func (carfileStore *CarfileStore) Stat() (fsutil.FsStat, error) {
	return carfileStore.blockStore.Stat()
}

func (carfileStore *CarfileStore) GetAllBlocksHash() ([]string, error) {
	return carfileStore.blockStore.GetAllKeys()
}

func (carfileStore *CarfileStore) BlockCount() (int, error) {
	return carfileStore.blockStore.KeyCount()
}

// carfile table
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

func (carfileStore *CarfileStore) GetPath() string {
	return carfileStore.path
}

func (carfileStore *CarfileStore) SaveBlockListOfCarfile(carfileHash string, blocksHashString string) error {
	return carfileStore.carfileTable.saveBlockListOfCarfile(carfileHash, blocksHashString)
}

func (carfileStore *CarfileStore) DeleteCarfileTable(carfileHash string) error {
	return carfileStore.carfileTable.delete(carfileHash)
}

func (carfileStore *CarfileStore) GetBlocksHashWithCarfilePositions(carfileHash string, positions []int) ([]string, error) {
	return carfileStore.carfileTable.readBlocksHashWithCarfilePosition(carfileHash, positions)
}

func (carfileStore *CarfileStore) GetBlocksHashOfCarfile(carfileHash string) ([]string, error) {
	return carfileStore.carfileTable.readAllBlocksHashOfCarfile(carfileHash)
}

func (carfileStore *CarfileStore) HasCarfile(carfileHash string) (bool, error) {
	return carfileStore.carfileTable.has(carfileHash)
}

func (carfileStore *CarfileStore) BlockCountOfCarfile(carfileHash string) (int, error) {
	return carfileStore.carfileTable.blockCountOfCarfile(carfileHash)
}

func (carfileStore *CarfileStore) GetCompleteCarfileHashList() ([]string, error) {
	return carfileStore.carfileTable.carfileHashList()
}

// incomplete carfileCache
func (carfileStore *CarfileStore) SaveIncompleteCarfileCache(carfileHash string, carfileCacheData []byte) error {
	return carfileStore.incompleteCarfileCache.saveCarfile(carfileHash, carfileCacheData)
}

func (carfileStore *CarfileStore) DeleteIncompleteCarfileCache(carfileHash string) error {
	return carfileStore.incompleteCarfileCache.delete(carfileHash)
}

func (carfileStore *CarfileStore) GetIncompleteCarfileCacheData(carfileHash string) ([]byte, error) {
	return carfileStore.incompleteCarfileCache.getCarfileCacheData(carfileHash)
}

func (carfileStore *CarfileStore) GetIncompleteCarfileHashList() ([]string, error) {
	return carfileStore.incompleteCarfileCache.carfileHashList()
}

// wait list file
func (carfileStore *CarfileStore) SaveWaitListToFile(data []byte) error {
	return saveWaitListToFile(data, filepath.Join(carfileStore.path, waitCacheListFile))
}

func (carfileStore *CarfileStore) GetWaitListFromFile() ([]byte, error) {
	return getWaitListFromFile(filepath.Join(carfileStore.path, waitCacheListFile))
}

// links
func (carfileStore *CarfileStore) SaveLinks(ctx context.Context, blockHash string, links []byte) error {
	return carfileStore.linksStore.put(ctx, blockHash, links)
}

func (carfileStore *CarfileStore) GetLinks(ctx context.Context, blockHash string) ([]byte, error) {
	return carfileStore.linksStore.get(ctx, blockHash)
}

func (carfileStore *CarfileStore) DeleteLinks(ctx context.Context, blockHash string) error {
	return carfileStore.linksStore.delete(ctx, blockHash)
}
