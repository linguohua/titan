package storage

import (
	"context"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-libipfs/blocks"
	"github.com/ipld/go-car/v2/blockstore"
	"golang.org/x/xerrors"
)

// asset save asset file
type asset struct {
	baseDir string
	suffix  string
}

// NewAsset initializes a new Asset instance.
func newAsset(baseDir, suffix string) (*asset, error) {
	err := os.MkdirAll(baseDir, 0o755)
	if err != nil {
		return nil, err
	}

	return &asset{baseDir: baseDir, suffix: suffix}, nil
}

// generateAssetName creates a new asset file name.
func (a *asset) newAssetName(root cid.Cid) string {
	return root.Hash().String() + a.suffix
}

// StoreBlocks stores blocks to the file system.
func (a *asset) putBlocks(ctx context.Context, root cid.Cid, blks []blocks.Block) error {
	assetDir := filepath.Join(a.baseDir, root.Hash().String())
	err := os.MkdirAll(assetDir, 0o755)
	if err != nil {
		return err
	}

	for _, blk := range blks {
		filePath := filepath.Join(assetDir, blk.Cid().Hash().String())
		if err := os.WriteFile(filePath, blk.RawData(), 0o644); err != nil {
			return err
		}
	}

	return nil
}

// StoreAsset stores the asset to the file system.
func (a *asset) putAsset(ctx context.Context, root cid.Cid) error {
	assetDir := filepath.Join(a.baseDir, root.Hash().String())
	entries, err := os.ReadDir(assetDir)
	if err != nil {
		return err
	}

	name := a.newAssetName(root)
	path := filepath.Join(a.baseDir, name)

	rw, err := blockstore.OpenReadWrite(path, []cid.Cid{root})
	if err != nil {
		return err
	}

	for _, entry := range entries {
		data, err := ioutil.ReadFile(filepath.Join(assetDir, entry.Name()))
		if err != nil {
			return err
		}

		blk := blocks.NewBlock(data)
		if err = rw.Put(ctx, blk); err != nil {
			return err
		}
	}

	if err = rw.Finalize(); err != nil {
		return err
	}

	return os.RemoveAll(assetDir)
}

// get must close reader
// Retrieve returns a ReadSeekCloser for the given asset root.
// The caller must close the reader.
func (a *asset) get(root cid.Cid) (io.ReadSeekCloser, error) {
	// check if put asset complete
	assetDir := filepath.Join(a.baseDir, root.Hash().String())
	if _, err := os.Stat(assetDir); err != nil {
		if !os.IsNotExist(err) {
			return nil, err
		}
	} else {
		return nil, xerrors.Errorf("putting asset, not ready")
	}

	name := a.newAssetName(root)
	filePath := filepath.Join(a.baseDir, name)
	return os.Open(filePath)
}

// Exists checks if the asset exists in the file system.
func (a *asset) has(root cid.Cid) (bool, error) {
	assetDir := filepath.Join(a.baseDir, root.Hash().String())
	if _, err := os.Stat(assetDir); err != nil {
		if !os.IsNotExist(err) {
			return false, err
		}
	} else {
		return false, nil
	}

	name := a.newAssetName(root)
	filePath := filepath.Join(a.baseDir, name)

	_, err := os.Stat(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}

		return false, err
	}

	return true, nil
}

// Remove deletes the asset from the file system.
func (a *asset) remove(root cid.Cid) error {
	name := a.newAssetName(root)
	path := filepath.Join(a.baseDir, name)

	// remove file
	return os.Remove(path)
}

// Count returns the number of assets in the file system.
func (a *asset) count() (int, error) {
	entries, err := os.ReadDir(a.baseDir)
	if err != nil {
		return 0, err
	}

	return len(entries), nil
}
