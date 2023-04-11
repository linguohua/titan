package storage

import (
	"os"
	"path/filepath"

	"github.com/ipfs/go-cid"
)

// assetPuller save assetPuller
// AssetStorage stores asset data using the filesystem.
type assetPuller struct {
	baseDir string
}

// NewAssetStorage initializes a new AssetStorage instance.
func newAssetPuller(baseDir string) (*assetPuller, error) {
	err := os.MkdirAll(baseDir, 0o755)
	if err != nil {
		return nil, err
	}

	return &assetPuller{baseDir: baseDir}, nil
}

// Store writes asset data to the filesystem.
func (cc *assetPuller) put(c cid.Cid, data []byte) error {
	filePath := filepath.Join(cc.baseDir, c.Hash().String())
	return os.WriteFile(filePath, data, 0o644)
}

// Retrieve reads asset data from the filesystem.
func (cc *assetPuller) get(c cid.Cid) ([]byte, error) {
	filePath := filepath.Join(cc.baseDir, c.Hash().String())
	return os.ReadFile(filePath)
}

// Exists checks if the asset data is stored in the filesystem.
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

// Remove deletes asset data from the filesystem.
func (cc *assetPuller) delete(c cid.Cid) error {
	filePath := filepath.Join(cc.baseDir, c.Hash().String())
	return os.Remove(filePath)
}
