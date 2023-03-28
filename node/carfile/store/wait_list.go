package store

import (
	"os"

	"github.com/ipfs/go-datastore"
)

type WaitList struct {
	path string
}

func (wl *WaitList) SaveWaitList(data []byte) error {
	return os.WriteFile(wl.path, data, 0644)
}

func (wl *WaitList) WaitList() ([]byte, error) {
	data, err := os.ReadFile(wl.path)
	if err != nil && os.IsNotExist(err) {
		return nil, datastore.ErrNotFound
	}

	return data, err
}
