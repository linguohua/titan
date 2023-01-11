package carfilestore

import (
	"os"
	"path/filepath"

	"github.com/ipfs/go-datastore"
)

// save block list of carfile
type carfileTable struct {
	path string
}

func newCarfileTable(path string) *carfileTable {
	err := os.MkdirAll(path, 0o755)
	if err != nil {
		log.Fatalf("New blocks dir, path:%s, err:%s", path, err.Error())
	}

	return &carfileTable{path: path}
}

func (cfTable *carfileTable) saveBlockListOfCarfile(carfileHash string, blocksHashString string) error {
	filePath := filepath.Join(cfTable.path, carfileHash)
	return os.WriteFile(filePath, []byte(blocksHashString), 0644)
}

func (cfTable *carfileTable) readBlocksHashOfCarfile(carfileHash string, positions []int) ([]string, error) {
	filePath := filepath.Join(cfTable.path, carfileHash)
	tableFile, err := os.Open(filePath)
	if err != nil {
		log.Fatal(err)
	}

	defer tableFile.Close()

	cids := make([]string, 0, len(positions))
	hashStringLen := len(carfileHash)
	for _, pos := range positions {
		_, err := tableFile.Seek(int64(pos*hashStringLen), 0)
		if err != nil {
			return nil, err
		}

		buffer := make([]byte, hashStringLen)
		_, err = tableFile.Read(buffer)
		if err != nil {
			return nil, err
		}

		cids = append(cids, string(buffer))
	}

	return cids, nil
}

func (cfTable *carfileTable) carfileCount() (int, error) {
	dir, err := os.Open(cfTable.path)
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

func (cfTable *carfileTable) delete(carfileHash string) error {
	filePath := filepath.Join(cfTable.path, carfileHash)

	err := os.Remove(filePath)
	if err != nil && os.IsNotExist(err) {
		return datastore.ErrNotFound
	}

	return err
}
