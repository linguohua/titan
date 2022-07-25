package redishelper

import (
	"sync"
	"time"

	"github.com/gomodule/redigo/redis"
)

var (
	pool *redis.Pool

	once sync.Once
)

const (
	// RedisKeyDeviceInfo  deviceID
	RedisKeyDeviceInfo = "Titan:DeviceInfo:%s"
)

// InitPool init redis pool
func InitPool(addr string) {
	pool = &redis.Pool{
		MaxIdle:     3,
		IdleTimeout: 240 * time.Second,
		Dial:        func() (redis.Conn, error) { return redis.Dial("tcp", addr) },
	}
}

// getConn get a redis connection
func getConn() redis.Conn {
	return pool.Get()
}
