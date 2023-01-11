package carfilestore

import (
	"os"
	"path/filepath"

	"github.com/ipfs/go-datastore"
	logging "github.com/ipfs/go-log/v2"
	"github.com/linguohua/titan/blockstore"
	"github.com/linguohua/titan/node/fsutil"
)

var log = logging.Logger("carfilestore")

const (
	blocksDir             = "blocks"
	carfileTableDir       = "carfiles"
	incompleteCarfilesDir = "incomplete-carfiles"
	waitCacheListFile     = "wait-cache"
)

type CarfileStore struct {
	blockStore             blockstore.BlockStore
	carfileTable           *carfileTable
	imcompleteCarfileTable *incompleteCarfileTable
	path                   string
}

func NewCarfileStore(path, blockStoreType string) *CarfileStore {
	blocksPath := filepath.Join(path, blocksDir)
	blockStore := blockstore.NewBlockStore(blocksPath, blockStoreType)

	carfileTablePath := filepath.Join(path, carfileTableDir)
	carfileTable := newCarfileTable(carfileTablePath)

	incompleteCarfilePath := filepath.Join(path, incompleteCarfilesDir)
	incompleteCarfileTable := newIncompleteCarfileTable(incompleteCarfilePath)

	return &CarfileStore{blockStore: blockStore, carfileTable: carfileTable, imcompleteCarfileTable: incompleteCarfileTable, path: path}
}

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
	return nil, nil
}

func (carfileStore *CarfileStore) BlocksCount() (int, error) {
	return 0, nil
}

func (carfileStore *CarfileStore) CarfilesCount() (int, error) {
	count1, err := carfileStore.carfileTable.carfileCount()
	if err != nil {
		return 0, err
	}

	count2, err := carfileStore.imcompleteCarfileTable.carfileCount()
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

func (carfileStore *CarfileStore) SaveIncomleteCarfile(carfileHash string, carfileData []byte) error {
	return carfileStore.imcompleteCarfileTable.saveCarfile(carfileHash, carfileData)
}

func (carfileStore *CarfileStore) DeleteCarfileTable(carfileHash string) error {
	return carfileStore.carfileTable.delete(carfileHash)
}

func (carfileStore *CarfileStore) DeleteIncompleteCarfile(carfileHash string) error {
	return carfileStore.imcompleteCarfileTable.delete(carfileHash)
}

func (carfileStore *CarfileStore) GetBlocksHashOfCarfile(carfileHash string, positions []int) ([]string, error) {
	return carfileStore.carfileTable.readBlocksHashOfCarfile(carfileHash, positions)
}

func (carfileStore *CarfileStore) SaveWaitList2File(data []byte) error {
	filePath := filepath.Join(carfileStore.path, waitCacheListFile)
	return os.WriteFile(filePath, data, 0644)
}

func (carfileStore *CarfileStore) GetWaitListFromFile() ([]byte, error) {
	filePath := filepath.Join(carfileStore.path, waitCacheListFile)

	data, err := os.ReadFile(filePath)
	if err != nil && os.IsNotExist(err) {
		return nil, datastore.ErrNotFound
	}

	return data, err
}
