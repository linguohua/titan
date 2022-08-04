package stores

import (
	"os"
	"path/filepath"
)

type fileStore struct {
	Path string
}

func (fs fileStore) Type() string {
	return "FileStore"
}

func (fs fileStore) Put(key string, value []byte) error {
	filePath := filepath.Join(fs.Path, key)
	return os.WriteFile(filePath, value, 0644)
}

func (fs fileStore) Get(key string) ([]byte, error) {
	filePath := filepath.Join(fs.Path, key)
	return os.ReadFile(filePath)
}

func (fs fileStore) Delete(key string) error {
	filePath := filepath.Join(fs.Path, key)
	return os.Remove(filePath)
}

func (fs fileStore) GetReader(key string) (BlockReader, error) {
	filePath := filepath.Join(fs.Path, key)
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}

	return file, nil
}

func (fs fileStore) Has(key string) (exists bool, err error) {
	filePath := filepath.Join(fs.Path, key)
	_, err = os.Stat(filePath)
	if err == nil {
		return true, nil
	}

	if os.IsNotExist(err) {
		return false, nil
	}

	return false, err
}

func (fs fileStore) GetSize(key string) (size int, err error) {
	filePath := filepath.Join(fs.Path, key)
	info, err := os.Stat(filePath)
	if err != nil {
		return 0, err
	}

	return int(info.Size()), nil
}
