package scheduler

import (
	"context"
	"fmt"

	"titan/node/scheduler/redishelper"

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

// DeviceCacheResult Device Cache Result
func DeviceCacheResult(deviceID, cid string, isOk bool, tag int) error {
	if !isOk {
		keyDeviceData := fmt.Sprintf(redishelper.RedisKeyDeviceDatas, deviceID)
		return redishelper.RedisHDEL(keyDeviceData, cid)
	}

	// keyDataDeviceList := fmt.Sprintf(redishelper.RedisKeyDataDeviceList, cid)

	return nil
}

// DeviceCacheInit Device Cache init
func DeviceCacheInit(deviceID, cid string, tag int) error {
	keyDeviceData := fmt.Sprintf(redishelper.RedisKeyDeviceDatas, deviceID)
	return redishelper.RedisHSET(keyDeviceData, cid, tag)
}
