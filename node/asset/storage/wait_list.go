package storage

import (
	"os"

	"github.com/ipfs/go-datastore"
)

type waitList struct {
	path string
}

func newWaitList(path string) *waitList {
	return &waitList{path: path}
}
func (wl *waitList) put(data []byte) error {
	return os.WriteFile(wl.path, data, 0644)
}

func (wl *waitList) get() ([]byte, error) {
	data, err := os.ReadFile(wl.path)
	if err != nil && os.IsNotExist(err) {
		return nil, datastore.ErrNotFound
	}

	return data, err
}
