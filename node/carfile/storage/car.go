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

// Car save car file
type car struct {
	baseDir string
	suffix  string
}

func newCar(baseDir, suffix string) (*car, error) {
	err := os.MkdirAll(baseDir, 0755)
	if err != nil {
		return nil, err
	}

	return &car{baseDir: baseDir, suffix: suffix}, nil
}

func newCarName(root cid.Cid) string {
	return root.Hash().String() + carSuffix
}

func (c *car) putBlocks(ctx context.Context, root cid.Cid, blks []blocks.Block) error {
	carDir := filepath.Join(c.baseDir, root.Hash().String())
	err := os.MkdirAll(carDir, 0755)
	if err != nil {
		return err
	}

	for _, blk := range blks {
		filePath := filepath.Join(carDir, blk.Cid().Hash().String())
		if err := os.WriteFile(filePath, blk.RawData(), 0644); err != nil {
			return err
		}
	}

	return nil
}

//
func (c *car) putCar(ctx context.Context, root cid.Cid) error {
	carDir := filepath.Join(c.baseDir, root.Hash().String())
	entries, err := os.ReadDir(carDir)
	if err != nil {
		return err
	}

	name := newCarName(root)
	path := filepath.Join(c.baseDir, name)

	rw, err := blockstore.OpenReadWrite(path, []cid.Cid{root})
	if err != nil {
		return err
	}

	for _, entry := range entries {
		data, err := ioutil.ReadFile(filepath.Join(carDir, entry.Name()))
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

	return os.RemoveAll(carDir)
}

// CarReader must close reader
func (c *car) get(root cid.Cid) (io.ReadSeekCloser, error) {
	carDir := filepath.Join(c.baseDir, root.Hash().String())
	if _, err := os.Stat(carDir); err != nil {
		if !os.IsNotExist(err) {
			return nil, err
		}
	} else {
		return nil, xerrors.Errorf("putting car, not ready")
	}

	name := newCarName(root)
	filePath := filepath.Join(c.baseDir, name)
	return os.Open(filePath)
}

func (c *car) has(root cid.Cid) (bool, error) {
	carDir := filepath.Join(c.baseDir, root.Hash().String())
	if _, err := os.Stat(carDir); err != nil {
		if !os.IsNotExist(err) {
			return false, err
		}
	} else {
		return false, nil
	}

	name := newCarName(root)
	filePath := filepath.Join(c.baseDir, name)

	_, err := os.Stat(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}

		return false, err
	}

	return true, nil
}

func (c *car) remove(root cid.Cid) error {
	name := newCarName(root)
	path := filepath.Join(c.baseDir, name)

	// remove file
	return os.Remove(path)
}

func (c *car) count() (int, error) {
	entries, err := os.ReadDir(c.baseDir)
	if err != nil {
		return 0, err
	}

	return len(entries), nil
}
