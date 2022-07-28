package scheduler

import (
	"context"
	"fmt"

	"github.com/linguohua/titan/node/scheduler/db"

	"github.com/gomodule/redigo/redis"
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

// NodeCacheResult Device Cache Result
func NodeCacheResult(deviceID, cid string, isOk bool, tag int) error {
	keyDataDeviceList := fmt.Sprintf(db.RedisKeyDataNodeList, cid)
	if !isOk {
		keyDeviceData := fmt.Sprintf(db.RedisKeyNodeDatas, deviceID)
		err := cacheDB.HDel(keyDeviceData, cid)
		if err != nil {
			return err
		}

		return cacheDB.SremSet(keyDataDeviceList, deviceID)
	}

	return cacheDB.AddSet(keyDataDeviceList, deviceID)
}

// NodeCacheReady Node Cache ready
func NodeCacheReady(deviceID, cid string, tag int) error {
	keyDeviceData := fmt.Sprintf(db.RedisKeyNodeDatas, deviceID)
	return cacheDB.HSetValue(keyDeviceData, cid, tag)
}

// GetNodeWithData find device
func GetNodeWithData(cid string) (string, error) {
	keyDataDeviceList := fmt.Sprintf(db.RedisKeyDataNodeList, cid)

	deviceIDs, err := redis.Strings(cacheDB.SmemberSet(keyDataDeviceList))
	if err != nil {
		return "", err
	}

	if len(deviceIDs) <= 0 {
		return "", xerrors.New("not find device")
	}

	deviceID := ""
	for _, deviceID = range deviceIDs {
		// TODO 找出最近的 device
		log.Infof("GetDevicesWithData : %v", deviceID)
	}

	return deviceID, nil
}
