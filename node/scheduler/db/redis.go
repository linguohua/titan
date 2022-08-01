package db

import (
	"context"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
	"golang.org/x/xerrors"
)

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

	// redis field
	lastTimeField   = "LastTime"
	onLineTimeField = "OnLineTime"
	geoField        = "Geo"
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

// //  hget
// func (rd redisDB) HGetValue(key, field string) (string, error) {
// 	return rd.cli.HGet(context.Background(), key, field).Result()
// }

// //  hset
// func (rd redisDB) HSetValue(key, field string, value interface{}) error {
// 	_, err := rd.cli.HSet(context.Background(), key, field, value).Result()
// 	return err
// }

// //  hmget
// func (rd redisDB) HGetValues(key string, args ...string) ([]interface{}, error) {
// 	return rd.cli.HMGet(context.Background(), key, args...).Result()
// }

// //  hmset
// func (rd redisDB) HSetValues(key string, args ...interface{}) error {
// 	_, err := rd.cli.HMSet(context.Background(), key, args).Result()
// 	return err
// }

// //  hdel
// func (rd redisDB) HDel(key, field string) error {
// 	_, err := rd.cli.HDel(context.Background(), key, field).Result()
// 	return err
// }

// // HIncrBy
// func (rd redisDB) IncrbyField(key, field string, value int64) error {
// 	_, err := rd.cli.HIncrBy(context.Background(), key, field, value).Result()
// 	return err
// }

// //  INCRBY
// func (rd redisDB) Incrby(key string, value int64) (int64, error) {
// 	return rd.cli.IncrBy(context.Background(), key, value).Result()
// }

// //  add
// func (rd redisDB) AddSet(key, value string) error {
// 	_, err := rd.cli.SAdd(context.Background(), key, value).Result()
// 	return err
// }

// // SMembers
// func (rd redisDB) SmemberSet(key string) ([]string, error) {
// 	return rd.cli.SMembers(context.Background(), key).Result()
// }

// // SRem
// func (rd redisDB) SremSet(key, value string) error {
// 	_, err := rd.cli.SRem(context.Background(), key, value).Result()
// 	return err
// }

// node cache tag ++1
func (rd redisDB) GetNodeCacheTag(deviceID string) (int64, error) {
	key := fmt.Sprintf(RedisKeyNodeDataTag, deviceID)

	return rd.cli.IncrBy(context.Background(), key, 1).Result()
}

// del node data with cid
func (rd redisDB) DelCacheDataInfo(deviceID, cid string) error {
	key := fmt.Sprintf(RedisKeyNodeDatas, deviceID)

	_, err := rd.cli.HDel(context.Background(), key, cid).Result()
	return err
}

// set cid
func (rd redisDB) SetCacheDataInfo(deviceID, cid string, tag int64) error {
	key := fmt.Sprintf(RedisKeyNodeDatas, deviceID)

	_, err := rd.cli.HSet(context.Background(), key, cid, tag).Result()
	return err
}

// get tag
func (rd redisDB) GetCacheDataInfo(deviceID, cid string) (string, error) {
	key := fmt.Sprintf(RedisKeyNodeDatas, deviceID)

	return rd.cli.HGet(context.Background(), key, cid).Result()
}

//  add
func (rd redisDB) SetNodeToCacheList(deviceID, cid string) error {
	key := fmt.Sprintf(RedisKeyDataNodeList, cid)

	_, err := rd.cli.SAdd(context.Background(), key, deviceID).Result()
	return err
}

// SMembers
func (rd redisDB) GetNodesWithCacheList(cid string) ([]string, error) {
	key := fmt.Sprintf(RedisKeyDataNodeList, cid)

	return rd.cli.SMembers(context.Background(), key).Result()
}

//  del
func (rd redisDB) DelNodeWithCacheList(deviceID, cid string) error {
	key := fmt.Sprintf(RedisKeyDataNodeList, cid)

	_, err := rd.cli.SRem(context.Background(), key, deviceID).Result()
	return err
}

func (rd redisDB) SetNodeInfo(deviceID string, info NodeInfo) error {
	key := fmt.Sprintf(RedisKeyNodeInfo, deviceID)

	_, err := rd.cli.HMSet(context.Background(), key, lastTimeField, info.LastTime, geoField, info.Geo).Result()
	if err != nil {
		return err
	}

	_, err = rd.cli.HIncrBy(context.Background(), key, onLineTimeField, info.OnLineTime).Result()
	return err
}

func (rd redisDB) GetNodeInfo(deviceID string) (NodeInfo, error) {
	key := fmt.Sprintf(RedisKeyNodeInfo, deviceID)

	vals, err := rd.cli.HMGet(context.Background(), key, geoField, onLineTimeField, lastTimeField).Result()
	if err != nil {
		return NodeInfo{}, err
	}

	if len(vals) <= 0 {
		return NodeInfo{}, xerrors.New("info not find")
	}

	g := vals[0].(string)
	o := vals[1].(int64)
	l := vals[2].(string)

	return NodeInfo{Geo: g, OnLineTime: o, LastTime: l}, nil
}

//  add
func (rd redisDB) SetNodeToGeoList(deviceID, geo string) error {
	key := fmt.Sprintf(RedisKeyGeoNodeList, geo)

	_, err := rd.cli.SAdd(context.Background(), key, deviceID).Result()
	return err
}

// SMembers
func (rd redisDB) GetNodesWithGeoList(geo string) ([]string, error) {
	key := fmt.Sprintf(RedisKeyGeoNodeList, geo)

	return rd.cli.SMembers(context.Background(), key).Result()
}

//  del
func (rd redisDB) DelNodeWithGeoList(deviceID, geo string) error {
	key := fmt.Sprintf(RedisKeyGeoNodeList, geo)

	_, err := rd.cli.SRem(context.Background(), key, deviceID).Result()
	return err
}
