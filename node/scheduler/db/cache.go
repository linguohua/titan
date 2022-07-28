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
	HGetValue(key, field string, out interface{}) (bool, error)
	HSetValue(key, field string, value interface{}) error
	HGetValues(key string, args ...string) ([]interface{}, error)
	HSetValues(key string, args ...interface{}) error
	HDel(key, field string) error
	IncrbyField(key, field string, value int64) error
	Incrby(key string, value int64) (int64, error)
	AddSet(key, deviceID string) error
	SmemberSet(key string) ([]string, error)
	SremSet(key, deviceID string) error
}

// NewCacheDB New Cache DB
func NewCacheDB(url string, dbType string) CacheDB {
	switch dbType {
	case TypeRedis():
		return InitRedis(url)

	default:
		panic("unknown CacheDB type")
	}
}
