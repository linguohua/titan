package db

import "fmt"

// CacheDB cache db
type CacheDB interface {
	// HGetValue(key, field string) (string, error)
	// HSetValue(key, field string, value interface{}) error
	// HGetValues(key string, args ...string) ([]interface{}, error)
	// HSetValues(key string, args ...interface{}) error
	// HDel(key, field string) error
	// IncrbyField(key, field string, value int64) error
	// Incrby(key string, value int64) (int64, error)
	// AddSet(key, value string) error
	// SmemberSet(key string) ([]string, error)
	// SremSet(key, value string) error

	GetNodeCacheTag(deviceID string) (int64, error)

	DelCacheDataInfo(deviceID, cid string) error
	SetCacheDataInfo(deviceID, cid string, tag int64) error
	GetCacheDataInfo(deviceID, cid string) (string, error)
	GetCacheDataInfos(deviceID string) (map[string]string, error)

	DelNodeWithCacheList(deviceID, cid string) error
	SetNodeToCacheList(deviceID, cid string) error
	GetNodesWithCacheList(cid string) ([]string, error)
	IsNodeInCacheList(cid, deviceID string) (bool, error)

	SetNodeInfo(deviceID string, info NodeInfo) error
	GetNodeInfo(deviceID string) (NodeInfo, error)

	DelNodeWithGeoList(deviceID, geo string) error
	SetNodeToGeoList(deviceID, geo string) error
	GetNodesWithGeoList(geo string) ([]string, error)
}

var cacheDB CacheDB

// NewCacheDB New Cache DB
func NewCacheDB(url string, dbType string) {
	var err error

	switch dbType {
	case TypeRedis():
		cacheDB, err = InitRedis(url)
	default:
		panic("unknown CacheDB type")
	}

	if err != nil {
		e := fmt.Sprintf("NewCacheDB err:%v , url:%v", err, url)
		panic(e)
	}
}

// GetCacheDB Get CacheDB
func GetCacheDB() CacheDB {
	return cacheDB
}

// NodeInfo base info
type NodeInfo struct {
	OnLineTime int64
	LastTime   string
	Geo        string
}
