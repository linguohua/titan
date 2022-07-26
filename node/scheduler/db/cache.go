package db

const (
	// RedisKeyDeviceInfo  deviceID
	RedisKeyDeviceInfo = "Titan:DeviceInfo:%s"

	// RedisKeyDeviceDatas  deviceID
	RedisKeyDeviceDatas = "Titan:DeviceDatas:%s"

	// RedisKeyDataDeviceList  cid
	RedisKeyDataDeviceList = "Titan:DataDeviceList:%s"

	// RedisKeyDeviceDataIncrby  deviceID
	RedisKeyDeviceDataIncrby = "Titan:DeviceDataIncrby:%s"
)

// CacheDB cache db
type CacheDB interface {
	init()
	HGetValue(key, field string) (interface{}, error)
	HSetValue(key, field string, value interface{}) error
	HGetValues(key string, args ...interface{}) (interface{}, error)
	HSetValues(key string, args ...interface{}) error
	HDel(key, field string) error
	IncrbyField(key, field string, value int) error
	Incrby(key string, value int) (int, error)
	AddSet(key, deviceID string) error
	SmemberSet(key string) (interface{}, error)
	SremSet(key, deviceID string) error
}

// Redis Redis
var Redis redisDB

// NewCacheDB New Cache DB
func NewCacheDB(url string, storeType string) CacheDB {
	switch storeType {
	case Redis.Type():
		Redis.URL = url
		Redis.init()
		return Redis

	default:
		panic("unknown CacheDB type")
	}
}
