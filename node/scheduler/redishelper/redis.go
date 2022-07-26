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

	// RedisKeyDeviceDatas  deviceID
	RedisKeyDeviceDatas = "Titan:DeviceDatas:%s"

	// RedisKeyDataDeviceList  cid
	RedisKeyDataDeviceList = "Titan:DataDeviceList:%s"

	// RedisKeyDeviceDataIncrby  deviceID
	RedisKeyDeviceDataIncrby = "Titan:DeviceDataIncrby:%s"
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

// RedisHGET hget
func RedisHGET(key, field string) (interface{}, error) {
	conn := getConn()
	defer conn.Close()

	return conn.Do("HGET", key, field)
}

// RedisHSET hset
func RedisHSET(key, field string, value interface{}) error {
	conn := getConn()
	defer conn.Close()

	_, err := conn.Do("HSET", key, field, value)

	return err
}

// RedisHMGET hmget
func RedisHMGET(key string, args ...interface{}) (interface{}, error) {
	conn := getConn()
	defer conn.Close()

	return conn.Do("HMGET", key, args)
}

// RedisHMSET hmset
func RedisHMSET(key string, args ...interface{}) error {
	conn := getConn()
	defer conn.Close()

	_, err := conn.Do("HMSET", key, args)

	return err
}

// RedisHDEL hdel
func RedisHDEL(key, field string) error {
	conn := getConn()
	defer conn.Close()

	_, err := conn.Do("HDEL", key, field)

	return err
}

// RedisHINCRBY HINCRBY
func RedisHINCRBY(key, field string, value int) error {
	conn := getConn()
	defer conn.Close()

	_, err := conn.Do("HINCRBY", key, field, value)

	return err
}

// RedisINCRBY INCRBY
func RedisINCRBY(key string, value int) (int, error) {
	conn := getConn()
	defer conn.Close()

	return redis.Int(conn.Do("INCRBY", key, value))
}
