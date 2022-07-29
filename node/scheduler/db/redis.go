package db

import (
	"context"
	"time"

	"github.com/go-redis/redis/v8"
)

// // RedisDB redis
// var RedisDB *redisDB

type redisDB struct {
	cli *redis.Client
}

// TypeRedis redis
func TypeRedis() string {
	return "Redis"
}

// InitRedis init redis pool
func InitRedis(url string) (CacheDB, error) {
	// fmt.Printf("redis init url : %v", url)

	redisDB := &redisDB{redis.NewClient(&redis.Options{
		Addr:      url,
		Dialer:    nil,
		OnConnect: nil,
	})}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := redisDB.cli.Ping(ctx).Result()

	return redisDB, err
}

//  hget
func (rd redisDB) HGetValue(key, field string) (string, error) {
	return rd.cli.HGet(context.Background(), key, field).Result()
}

//  hset
func (rd redisDB) HSetValue(key, field string, value interface{}) error {
	_, err := rd.cli.HSet(context.Background(), key, field, value).Result()
	return err
}

//  hmget
func (rd redisDB) HGetValues(key string, args ...string) ([]interface{}, error) {
	return rd.cli.HMGet(context.Background(), key, args...).Result()
}

//  hmset
func (rd redisDB) HSetValues(key string, args ...interface{}) error {
	_, err := rd.cli.HMSet(context.Background(), key, args).Result()
	return err
}

//  hdel
func (rd redisDB) HDel(key, field string) error {
	_, err := rd.cli.HDel(context.Background(), key, field).Result()
	return err
}

// HIncrBy
func (rd redisDB) IncrbyField(key, field string, value int64) error {
	_, err := rd.cli.HIncrBy(context.Background(), key, field, value).Result()
	return err
}

//  INCRBY
func (rd redisDB) Incrby(key string, value int64) (int64, error) {
	return rd.cli.IncrBy(context.Background(), key, value).Result()
}

//  add
func (rd redisDB) AddSet(key, value string) error {
	_, err := rd.cli.SAdd(context.Background(), key, value).Result()
	return err
}

// SMembers
func (rd redisDB) SmemberSet(key string) ([]string, error) {
	return rd.cli.SMembers(context.Background(), key).Result()
}

// SRem
func (rd redisDB) SremSet(key, value string) error {
	_, err := rd.cli.SRem(context.Background(), key, value).Result()
	return err
}
