package carfilestore

import (
	"os"
	"path/filepath"

	"github.com/ipfs/go-datastore"
)

// save block list of carfile
type incompleteCarfileTable struct {
	path string
}

func newIncompleteCarfileTable(path string) *incompleteCarfileTable {
	err := os.MkdirAll(path, 0o755)
	if err != nil {
		log.Fatalf("New blocks dir, path:%s, err:%s", path, err.Error())
	}

	return &incompleteCarfileTable{path: path}
}

func (inCfTable *incompleteCarfileTable) saveCarfile(carfileHash string, carfileData []byte) error {
	filePath := filepath.Join(inCfTable.path, carfileHash)
	return os.WriteFile(filePath, carfileData, 0644)
}

func (inCfTable *incompleteCarfileTable) getCarfile(carfileHash string) ([]byte, error) {
	filePath := filepath.Join(inCfTable.path, carfileHash)

	data, err := os.ReadFile(filePath)
	if err != nil && os.IsNotExist(err) {
		return nil, datastore.ErrNotFound
	}
	return data, err
}

func (inCfTable *incompleteCarfileTable) carfileCount() (int, error) {
	dir, err := os.Open(inCfTable.path)
	if err != nil {
		return 0, err
	}
	defer dir.Close() //nolint:errcheck

	files, err := dir.Readdir(-1)
	if err != nil {
		return 0, err
	}

	return len(files), nil
}

func (inCfTable *incompleteCarfileTable) delete(carfileHash string) error {
	filePath := filepath.Join(inCfTable.path, carfileHash)

	err := os.Remove(filePath)
	if err != nil && os.IsNotExist(err) {
		return datastore.ErrNotFound
	}

	return err
}
