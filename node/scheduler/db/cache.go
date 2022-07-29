package db

import "fmt"

const (
	// RedisKeyNodeInfo  deviceID
	RedisKeyNodeInfo = "Titan:NodeInfo:%s"
	// RedisKeyNodeDatas  deviceID
	RedisKeyNodeDatas = "Titan:NodeDatas:%s"
	// RedisKeyDataNodeList  cid
	RedisKeyDataNodeList = "Titan:DataNodeList:%s"
	// RedisKeyNodeDataTag  deviceID
	RedisKeyNodeDataTag = "Titan:NodeDataTag:%s"
	// RedisKeyGeoNodeList  isocode
	RedisKeyGeoNodeList = "Titan:GeoNodeList:%s"
)

// CacheDB cache db
type CacheDB interface {
	HGetValue(key, field string) (string, error)
	HSetValue(key, field string, value interface{}) error
	HGetValues(key string, args ...string) ([]interface{}, error)
	HSetValues(key string, args ...interface{}) error
	HDel(key, field string) error
	IncrbyField(key, field string, value int64) error
	Incrby(key string, value int64) (int64, error)
	AddSet(key, value string) error
	SmemberSet(key string) ([]string, error)
	SremSet(key, value string) error
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
