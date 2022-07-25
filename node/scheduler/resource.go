package scheduler

import (
	"context"

	"golang.org/x/xerrors"
)

// CacheData Cache Data
func CacheData(cids, deviceIDs []string) error {
	for _, deviceID := range deviceIDs {
		edge := getEdgeNode(deviceID)
		if edge == nil {
			continue
		}

		err := edge.edgeAPI.CacheData(context.Background(), cids)
		if err != nil {
			log.Errorf("CacheData err : %v", err)
		}
	}

	return nil
}

// LoadData Load Data
func LoadData(cid string, deviceID string) ([]byte, error) {
	edge := getEdgeNode(deviceID)
	if edge == nil {
		return nil, xerrors.New("not find edge")
	}

	// ...

	return nil, nil
}
