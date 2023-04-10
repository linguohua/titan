package storage

import (
	"os"
	"path/filepath"

	"github.com/ipfs/go-cid"
)

// assetPuller save assetPuller
type assetPuller struct {
	baseDir string
}

func newAssetPuller(baseDir string) (*assetPuller, error) {
	err := os.MkdirAll(baseDir, 0755)
	if err != nil {
		return nil, err
	}

	return &assetPuller{baseDir: baseDir}, nil
}

func (cc *assetPuller) put(c cid.Cid, data []byte) error {
	filePath := filepath.Join(cc.baseDir, c.Hash().String())
	return os.WriteFile(filePath, data, 0644)
}

func (cc *assetPuller) get(c cid.Cid) ([]byte, error) {
	filePath := filepath.Join(cc.baseDir, c.Hash().String())
	return os.ReadFile(filePath)
}

func (cc *assetPuller) has(c cid.Cid) (bool, error) {
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

func (cc *assetPuller) delete(c cid.Cid) error {
	filePath := filepath.Join(cc.baseDir, c.Hash().String())
	return os.Remove(filePath)
}
