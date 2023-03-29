package storage

import (
	"os"

	"github.com/ipfs/go-datastore"
)

type WaitList struct {
	path string
}

func NewWaitList(path string) *WaitList {
	return &WaitList{path: path}
}
func (wl *WaitList) Put(data []byte) error {
	return os.WriteFile(wl.path, data, 0644)
}

func (wl *WaitList) Get() ([]byte, error) {
	data, err := os.ReadFile(wl.path)
	if err != nil && os.IsNotExist(err) {
		return nil, datastore.ErrNotFound
	}

	return data, err
}
