package store

import (
	"os"
	"path/filepath"

	"github.com/ipfs/go-datastore"
)

// save block list of carfileCache
type incompleteCarfileCache struct {
	path string
}

func newIncompleteCarfileCache(path string) *incompleteCarfileCache {
	err := os.MkdirAll(path, 0o755)
	if err != nil {
		log.Panicf("New blocks dir, path:%s, err:%s", path, err.Error())
	}

	return &incompleteCarfileCache{path: path}
}

func (incompCarfileCache *incompleteCarfileCache) save(carfileHash string, carfileData []byte) error {
	filePath := filepath.Join(incompCarfileCache.path, carfileHash)
	return os.WriteFile(filePath, carfileData, 0644)
}

func (incompCarfileCache *incompleteCarfileCache) data(carfileHash string) ([]byte, error) {
	filePath := filepath.Join(incompCarfileCache.path, carfileHash)

	data, err := os.ReadFile(filePath)
	if err != nil && os.IsNotExist(err) {
		return nil, datastore.ErrNotFound
	}
	return data, err
}

func (incompCarfileCache *incompleteCarfileCache) has(carfileHash string) (bool, error) {
	filePath := filepath.Join(incompCarfileCache.path, carfileHash)
	_, err := os.Stat(filePath)
	if err == nil {
		return true, nil
	}

	if os.IsNotExist(err) {
		return false, nil
	}

	return false, err
}

func (incompCarfileCache *incompleteCarfileCache) carfileHashList() ([]string, error) {
	dir, err := os.Open(incompCarfileCache.path)
	if err != nil {
		return nil, err
	}
	defer dir.Close() //nolint:errcheck

	return dir.Readdirnames(-1)
}

func (incompCarfileCache *incompleteCarfileCache) delete(carfileHash string) error {
	filePath := filepath.Join(incompCarfileCache.path, carfileHash)

	err := os.Remove(filePath)
	if err != nil && os.IsNotExist(err) {
		return datastore.ErrNotFound
	}

	return err
}
