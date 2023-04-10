package storage

import (
	"os"
	"path/filepath"

	"github.com/ipfs/go-cid"
)

// assetCache save assetCache
type assetCache struct {
	baseDir string
}

func newAssetCache(baseDir string) (*assetCache, error) {
	err := os.MkdirAll(baseDir, 0755)
	if err != nil {
		return nil, err
	}

	return &assetCache{baseDir: baseDir}, nil
}

func (cc *assetCache) put(c cid.Cid, data []byte) error {
	filePath := filepath.Join(cc.baseDir, c.Hash().String())
	return os.WriteFile(filePath, data, 0644)
}

func (cc *assetCache) get(c cid.Cid) ([]byte, error) {
	filePath := filepath.Join(cc.baseDir, c.Hash().String())
	return os.ReadFile(filePath)
}

func (cc *assetCache) has(c cid.Cid) (bool, error) {
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

func (cc *assetCache) delete(c cid.Cid) error {
	filePath := filepath.Join(cc.baseDir, c.Hash().String())
	return os.Remove(filePath)
}
