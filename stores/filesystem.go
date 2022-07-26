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

func (fs fileStore) Put(data []byte, cid string) error {
	filePath := filepath.Join(fs.Path, cid)
	return os.WriteFile(filePath, data, 0644)
}

func (fs fileStore) Get(cid string) ([]byte, error) {
	filePath := filepath.Join(fs.Path, cid)
	return os.ReadFile(filePath)
}

func (fs fileStore) Delete(cid string) error {
	filePath := filepath.Join(fs.Path, cid)
	return os.Remove(filePath)
}
