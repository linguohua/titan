package carfilestore

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

func (incompCarfileCache *incompleteCarfileCache) saveCarfile(carfileHash string, carfileData []byte) error {
	filePath := filepath.Join(incompCarfileCache.path, carfileHash)
	return os.WriteFile(filePath, carfileData, 0644)
}

func (incompCarfileCache *incompleteCarfileCache) getCarfileCacheData(carfileHash string) ([]byte, error) {
	filePath := filepath.Join(incompCarfileCache.path, carfileHash)

	data, err := os.ReadFile(filePath)
	if err != nil && os.IsNotExist(err) {
		return nil, datastore.ErrNotFound
	}
	return data, err
}

func (incompCarfileCache *incompleteCarfileCache) carfileCount() (int, error) {
	dir, err := os.Open(incompCarfileCache.path)
	if err != nil {
		return 0, err
	}
	defer dir.Close()

	files, err := dir.Readdir(-1)
	if err != nil {
		return 0, err
	}

	return len(files), nil
}

func (incompCarfileCache *incompleteCarfileCache) delete(carfileHash string) error {
	filePath := filepath.Join(incompCarfileCache.path, carfileHash)

	err := os.Remove(filePath)
	if err != nil && os.IsNotExist(err) {
		return datastore.ErrNotFound
	}

	return err
}
