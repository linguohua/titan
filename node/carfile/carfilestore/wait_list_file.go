package carfilestore

import (
	"os"

	"github.com/ipfs/go-datastore"
)

func saveWaitListToFile(data []byte, path string) error {
	return os.WriteFile(path, data, 0644)
}

func getWaitListFromFile(path string) ([]byte, error) {
	data, err := os.ReadFile(path)
	if err != nil && os.IsNotExist(err) {
		return nil, datastore.ErrNotFound
	}

	return data, err
}
