package store

import (
	"os"
	"path/filepath"

	"github.com/ipfs/go-cid"
)

// save block list of CarCache
type CarCache struct {
	baseDir string
}

func NewCarCache(baseDir string) *CarCache {
	err := os.MkdirAll(baseDir, 0o755)
	if err != nil {
		log.Panicf("New car cache dir:%s, err:%s", baseDir, err.Error())
	}

	return &CarCache{baseDir: baseDir}
}

func (cc *CarCache) Put(c cid.Cid, data []byte) error {
	filePath := filepath.Join(cc.baseDir, c.Hash().String())
	return os.WriteFile(filePath, data, 0644)
}

func (cc *CarCache) Get(c cid.Cid) ([]byte, error) {
	filePath := filepath.Join(cc.baseDir, c.Hash().String())
	return os.ReadFile(filePath)
}

func (cc *CarCache) Has(c cid.Cid) (bool, error) {
	filePath := filepath.Join(cc.baseDir, c.Hash().String())
	_, err := os.Stat(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}

		return false, err
	}

	return true, nil
}

func (cc *CarCache) Delete(c cid.Cid) error {
	filePath := filepath.Join(cc.baseDir, c.Hash().String())
	return os.Remove(filePath)
}
