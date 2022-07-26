package db

import (
	"time"

	"github.com/gomodule/redigo/redis"
)

type redisDB struct {
	URL  string
	pool *redis.Pool
}

func (rd redisDB) Type() string {
	return "Redis"
}

// InitPool init redis pool
func (rd redisDB) init() {
	rd.pool = &redis.Pool{
		MaxIdle:     3,
		IdleTimeout: 240 * time.Second,
		Dial:        func() (redis.Conn, error) { return redis.Dial("tcp", rd.URL) },
	}
}

// getConn get a redis connection
func (rd redisDB) getConn() redis.Conn {
	return rd.pool.Get()
}

// RedisHGET hget
func (rd redisDB) HGetValue(key, field string) (interface{}, error) {
	conn := rd.getConn()
	defer conn.Close()

	return conn.Do("HGET", key, field)
}

// RedisHSET hset
func (rd redisDB) HSetValue(key, field string, value interface{}) error {
	conn := rd.getConn()
	defer conn.Close()

	_, err := conn.Do("HSET", key, field, value)

	return err
}

// RedisHMGET hmget
func (rd redisDB) HGetValues(key string, args ...interface{}) (interface{}, error) {
	conn := rd.getConn()
	defer conn.Close()

	return conn.Do("HMGET", key, args)
}

// RedisHMSET hmset
func (rd redisDB) HSetValues(key string, args ...interface{}) error {
	conn := rd.getConn()
	defer conn.Close()

	_, err := conn.Do("HMSET", key, args)

	return err
}

// RedisHDEL hdel
func (rd redisDB) HDel(key, field string) error {
	conn := rd.getConn()
	defer conn.Close()

	_, err := conn.Do("HDEL", key, field)

	return err
}

// RedisHINCRBY HINCRBY
func (rd redisDB) IncrbyField(key, field string, value int) error {
	conn := rd.getConn()
	defer conn.Close()

	_, err := conn.Do("HINCRBY", key, field, value)

	return err
}

// RedisINCRBY INCRBY
func (rd redisDB) Incrby(key string, value int) (int, error) {
	conn := rd.getConn()
	defer conn.Close()

	return redis.Int(conn.Do("INCRBY", key, value))
}

// RedisSADD add
func (rd redisDB) AddSet(key, deviceID string) error {
	conn := rd.getConn()
	defer conn.Close()

	_, err := conn.Do("SADD", key, deviceID)

	return err
}

// RedisSMEMBERS 返回集合中的所有成员
func (rd redisDB) SmemberSet(key string) (interface{}, error) {
	conn := rd.getConn()
	defer conn.Close()

	return conn.Do("SMEMBERS", key)
}

// RedisSREM 移除集合中一个或多个成员
func (rd redisDB) SremSet(key, deviceID string) error {
	conn := rd.getConn()
	defer conn.Close()

	_, err := conn.Do("SREM", key, deviceID)

	return err
}
