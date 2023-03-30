package storage

import (
	"os"
	"path/filepath"

	"github.com/ipfs/go-cid"
)

// CarCache save carfileCache
type carCache struct {
	baseDir string
}

func newCarCache(baseDir string) (*carCache, error) {
	err := os.MkdirAll(baseDir, 0o755)
	if err != nil {
		return nil, err
	}

	return &carCache{baseDir: baseDir}, nil
}

func (cc *carCache) put(c cid.Cid, data []byte) error {
	filePath := filepath.Join(cc.baseDir, c.Hash().String())
	return os.WriteFile(filePath, data, 0644)
}

func (cc *carCache) get(c cid.Cid) ([]byte, error) {
	filePath := filepath.Join(cc.baseDir, c.Hash().String())
	return os.ReadFile(filePath)
}

func (cc *carCache) has(c cid.Cid) (bool, error) {
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

func (cc *carCache) delete(c cid.Cid) error {
	filePath := filepath.Join(cc.baseDir, c.Hash().String())
	return os.Remove(filePath)
}
