package scheduler

import (
	"context"
	"fmt"

	"titan/node/scheduler/redishelper"

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

// DeviceCacheResult Device Cache Result
func DeviceCacheResult(deviceID, cid string, isOk bool, tag int) error {
	keyDataDeviceList := fmt.Sprintf(redishelper.RedisKeyDataDeviceList, cid)
	if !isOk {
		keyDeviceData := fmt.Sprintf(redishelper.RedisKeyDeviceDatas, deviceID)
		err := redishelper.RedisHDEL(keyDeviceData, cid)
		if err != nil {
			return err
		}

		return redishelper.RedisSREM(keyDataDeviceList, deviceID)
	}

	return redishelper.RedisSADD(keyDataDeviceList, deviceID)
}

// DeviceCacheInit Device Cache init
func DeviceCacheInit(deviceID, cid string, tag int) error {
	keyDeviceData := fmt.Sprintf(redishelper.RedisKeyDeviceDatas, deviceID)
	return redishelper.RedisHSET(keyDeviceData, cid, tag)
}

// GetDevicesWithData find device
func GetDevicesWithData(cid string) (string, error) {
	keyDataDeviceList := fmt.Sprintf(redishelper.RedisKeyDataDeviceList, cid)

	deviceIDs, err := redis.Strings(redishelper.RedisSMEMBERS(keyDataDeviceList))
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
