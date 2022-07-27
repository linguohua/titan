package db

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	// "github.com/gomodule/redigo/redis"

	"github.com/go-redis/redis/v8"
)

// RedisDB redis
var RedisDB *redisDB

type redisDB struct {
	// URL string
	// pool *redis.Pool

	cli *redis.Client
}

// TypeRedis redis
func TypeRedis() string {
	return "Redis"
}

// InitRedis init redis pool
func InitRedis(url string) CacheDB {
	fmt.Printf("redis init url : %v", url)

	// rd.pool = &redis.Pool{
	// 	MaxIdle:     3,
	// 	IdleTimeout: 240 * time.Second,
	// 	Dial:        func() (redis.Conn, error) { return redis.Dial("tcp", rd.URL) },
	// }
	RedisDB = &redisDB{redis.NewClient(&redis.Options{
		Addr:      url,
		Dialer:    nil,
		OnConnect: nil,
	})}

	return RedisDB
}

//  get a redis connection
// func (rd redisDB) getConn() redis.Conn {
// 	return rd.pool.Get()
// }

//  hget
func (rd redisDB) HGetValue(key, field string, out interface{}) (bool, error) {
	// conn := rd.getConn()
	// defer conn.Close()

	// return conn.Do("HGET", key, field)
	val, err := rd.cli.HGet(context.Background(), key, field).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return false, err
	}

	if val == "" {
		return false, nil
	}

	err = json.Unmarshal([]byte(val), &out)
	return true, err
}

//  hset
func (rd redisDB) HSetValue(key, field string, value interface{}) error {
	// conn := rd.getConn()
	// defer conn.Close()

	// _, err := conn.Do("HSET", key, field, value)

	_, err := rd.cli.HSet(context.Background(), key, field, value).Result()
	return err
}

//  hmget
// func (rd redisDB) HGetValues(key string, args ...interface{}) (interface{}, error) {
// 	conn := rd.getConn()
// 	defer conn.Close()

// 	return conn.Do("HMGET", key, args)
// }

//  hmset
func (rd redisDB) HSetValues(key string, args ...interface{}) error {
	// conn := rd.getConn()
	// defer conn.Close()

	// _, err := conn.Do("HMSET", key, args)

	_, err := rd.cli.HMSet(context.Background(), key, args).Result()
	return err
}

//  hdel
func (rd redisDB) HDel(key, field string) error {
	// conn := rd.getConn()
	// defer conn.Close()

	// _, err := conn.Do("HDEL", key, field)

	_, err := rd.cli.HDel(context.Background(), key, field).Result()
	return err
}

// RedisHINCRBY HINCRBY
func (rd redisDB) IncrbyField(key, field string, value int64) error {
	// conn := rd.getConn()
	// defer conn.Close()

	// _, err := conn.Do("HINCRBY", key, field, value)

	_, err := rd.cli.HIncrBy(context.Background(), key, field, value).Result()
	return err
}

// RedisINCRBY INCRBY
func (rd redisDB) Incrby(key string, value int64) (int64, error) {
	// conn := rd.getConn()
	// defer conn.Close()

	// return redis.Int(conn.Do("INCRBY", key, value))

	return rd.cli.IncrBy(context.Background(), key, value).Result()
}

// RedisSADD add
func (rd redisDB) AddSet(key, deviceID string) error {
	// conn := rd.getConn()
	// defer conn.Close()

	// _, err := conn.Do("SADD", key, deviceID)
	_, err := rd.cli.SAdd(context.Background(), key, deviceID).Result()
	return err
}

// RedisSMEMBERS 返回集合中的所有成员
func (rd redisDB) SmemberSet(key string) ([]string, error) {
	// conn := rd.getConn()
	// defer conn.Close()

	// return conn.Do("SMEMBERS", key)
	return rd.cli.SMembers(context.Background(), key).Result()
}

// RedisSREM 移除集合中一个或多个成员
func (rd redisDB) SremSet(key, deviceID string) error {
	// conn := rd.getConn()
	// defer conn.Close()

	// _, err := conn.Do("SREM", key, deviceID)

	_, err := rd.cli.SRem(context.Background(), key, deviceID).Result()
	return err
}
