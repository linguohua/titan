package store

import (
	"os"
	"path/filepath"
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

func (icc *incompleteCarfileCache) save(carfileHash string, carfileData []byte) error {
	filePath := filepath.Join(icc.path, carfileHash)
	return os.WriteFile(filePath, carfileData, 0644)
}

func (icc *incompleteCarfileCache) data(carfileHash string) ([]byte, error) {
	filePath := filepath.Join(icc.path, carfileHash)
	return os.ReadFile(filePath)
}

func (icc *incompleteCarfileCache) has(carfileHash string) (bool, error) {
	filePath := filepath.Join(icc.path, carfileHash)
	_, err := os.Stat(filePath)
	if err == nil {
		return true, nil
	}

	if os.IsNotExist(err) {
		return false, nil
	}

	return false, err
}

func (icc *incompleteCarfileCache) delete(carfileHash string) error {
	filePath := filepath.Join(icc.path, carfileHash)
	return os.Remove(filePath)
}
