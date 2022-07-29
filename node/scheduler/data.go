package scheduler

import (
	"context"
	"fmt"

	"github.com/linguohua/titan/geoip"
	"github.com/linguohua/titan/node/scheduler/db"

	"github.com/gomodule/redigo/redis"
	"golang.org/x/xerrors"
)

// CacheData Cache Data
func CacheData(cid, deviceID string) error {
	edge := getEdgeNode(deviceID)
	if edge == nil {
		return xerrors.New("node not find")
	}

	err := edge.edgeAPI.CacheData(context.Background(), []string{cid})
	if err != nil {
		log.Errorf("CacheData err : %v", err)
		return err
	}

	tag, err := getTagWithNode(deviceID)
	if err != nil {
		log.Errorf("CacheData getTagWithNode err : %v", err)
		return err
	}

	err = nodeCacheReady(deviceID, cid, tag)
	if err != nil {
		log.Errorf("CacheData nodeCacheReady err : %v", err)
		return err
	}

	return nil
}

// NodeCacheResult Device Cache Result
func nodeCacheResult(deviceID, cid string, isOk bool) error {
	if !isOk {
		keyDeviceData := fmt.Sprintf(db.RedisKeyNodeDatas, deviceID)
		return db.GetCacheDB().HDel(keyDeviceData, cid)
		// return db.GetCacheDB().SremSet(keyDataDeviceList, deviceID)
	}

	keyDataDeviceList := fmt.Sprintf(db.RedisKeyDataNodeList, cid)
	return db.GetCacheDB().AddSet(keyDataDeviceList, deviceID)
}

// Node Cache ready
func nodeCacheReady(deviceID, cid string, tag int64) error {
	keyDeviceData := fmt.Sprintf(db.RedisKeyNodeDatas, deviceID)
	return db.GetCacheDB().HSetValue(keyDeviceData, cid, tag)
}

func getTagWithNode(deviceID string) (int64, error) {
	key := fmt.Sprintf(db.RedisKeyNodeDataTag, deviceID)
	return db.GetCacheDB().Incrby(key, 1)
}

// GetNodeWithData find device
func GetNodeWithData(cid, ip string) (string, error) {
	keyDataDeviceList := fmt.Sprintf(db.RedisKeyDataNodeList, cid)

	deviceIDs, err := redis.Strings(db.GetCacheDB().SmemberSet(keyDataDeviceList))
	if err != nil {
		return "", err
	}

	if len(deviceIDs) <= 0 {
		return "", xerrors.New("not find node")
	}

	// TODO find node
	gInfo, err := geoip.GetGeoIP().GetGeoInfo(ip)
	if err != nil {
		log.Errorf("GetNodeWithData GetGeoInfo err : %v ,ip : %v", err, ip)
	}
	isoCode := gInfo.IsoCode

	deviceID := ""
	for _, dID := range deviceIDs {
		node := getEdgeNode(dID)
		if node == nil {
			continue
		}

		// TODO default
		deviceID = dID

		if node.geoInfo.IsoCode == isoCode {
			deviceID = dID
			break
		}
	}

	return deviceID, nil
}
