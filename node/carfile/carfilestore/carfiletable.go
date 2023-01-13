package carfilestore

import (
	"fmt"
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

func (cfTable *carfileTable) readAllBlocksHashOfCarfile(carfileHash string) ([]string, error) {
	filePath := filepath.Join(cfTable.path, carfileHash)
	data, err := os.ReadFile(filePath)
	if err != nil && os.IsNotExist(err) {
		return nil, datastore.ErrNotFound
	}

	blockHashStrLen := len(carfileHash)

	if len(data)%blockHashStrLen != 0 {
		return nil, fmt.Errorf("Carfile table content len not match")
	}

	blockCount := len(data) / blockHashStrLen
	blocksHash := make([]string, 0, blockCount)

	for i := 0; i < len(data); i += blockHashStrLen {
		hash := data[i : i+blockHashStrLen]
		blocksHash = append(blocksHash, string(hash))
	}

	return blocksHash, nil
}

func (cfTable *carfileTable) blocksCountOfCarfile(carfileHash string) (int, error) {
	filePath := filepath.Join(cfTable.path, carfileHash)
	data, err := os.ReadFile(filePath)
	if err != nil && os.IsNotExist(err) {
		return 0, datastore.ErrNotFound
	}

	blockHashStrLen := len(carfileHash)
	if len(data)%blockHashStrLen != 0 {
		return 0, fmt.Errorf("Carfile table content len not match")
	}

	return len(data) / blockHashStrLen, nil
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

func (cfTable *carfileTable) has(carfileHash string) (bool, error) {
	filePath := filepath.Join(cfTable.path, carfileHash)
	_, err := os.Stat(filePath)
	if err == nil {
		return true, nil
	}

	if os.IsNotExist(err) {
		return false, nil
	}

	return false, err
}
