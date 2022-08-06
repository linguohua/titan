package db

import (
	"context"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
	redigo "github.com/gomodule/redigo/redis"
	"github.com/linguohua/titan/api"
	"golang.org/x/xerrors"
)

const (
	// RedisKeyNodeInfo  deviceID
	redisKeyNodeInfo = "Titan:NodeInfo:%s"
	// RedisKeyNodeDatas  deviceID
	redisKeyNodeDatas = "Titan:NodeDatas:%s"
	// RedisKeyDataNodeList  cid
	redisKeyDataNodeList = "Titan:DataNodeList:%s"
	// RedisKeyNodeDataTag  deviceID
	redisKeyNodeDataTag = "Titan:NodeDataTag:%s"
	// RedisKeyGeoNodeList  geo
	redisKeyGeoNodeList = "Titan:GeoNodeList:%s"
	// RedisKeyNodeList  Edge/Candidate
	redisKeyNodeList = "Titan:NodeList:%s"
	// RedisKeyGeoList
	redisKeyGeoList = "Titan:GeoList"
	// RedisKeyValidatorList
	redisKeyValidatorList = "Titan:ValidatorList"
	// RedisKeyValidatorGeoList deviceID
	redisKeyValidatorGeoList = "Titan:ValidatorGeoList:%s"

	// redis field
	lastTimeField   = "LastTime"
	onLineTimeField = "OnLineTime"
	geoField        = "Geo"
	isOnlineField   = "IsOnline"
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
	key := fmt.Sprintf(redisKeyNodeDataTag, deviceID)

	return rd.cli.IncrBy(context.Background(), key, 1).Result()
}

// del node data with cid
func (rd redisDB) DelCacheDataInfo(deviceID, cid string) error {
	key := fmt.Sprintf(redisKeyNodeDatas, deviceID)

	_, err := rd.cli.HDel(context.Background(), key, cid).Result()
	return err
}

// set cid
func (rd redisDB) SetCacheDataInfo(deviceID, cid string, tag int64) error {
	key := fmt.Sprintf(redisKeyNodeDatas, deviceID)

	_, err := rd.cli.HSet(context.Background(), key, cid, tag).Result()
	return err
}

// get cache info
func (rd redisDB) GetCacheDataInfo(deviceID, cid string) (string, error) {
	key := fmt.Sprintf(redisKeyNodeDatas, deviceID)

	return rd.cli.HGet(context.Background(), key, cid).Result()
}

// get all cache info
func (rd redisDB) GetCacheDataInfos(deviceID string) (map[string]string, error) {
	key := fmt.Sprintf(redisKeyNodeDatas, deviceID)

	return rd.cli.HGetAll(context.Background(), key).Result()
}

//  add
func (rd redisDB) SetNodeToCacheList(deviceID, cid string) error {
	key := fmt.Sprintf(redisKeyDataNodeList, cid)

	_, err := rd.cli.SAdd(context.Background(), key, deviceID).Result()
	return err
}

// SMembers
func (rd redisDB) GetNodesWithCacheList(cid string) ([]string, error) {
	key := fmt.Sprintf(redisKeyDataNodeList, cid)

	return rd.cli.SMembers(context.Background(), key).Result()
}

// SISMEMBER
func (rd redisDB) IsNodeInCacheList(cid, deviceID string) (bool, error) {
	key := fmt.Sprintf(redisKeyDataNodeList, cid)

	return rd.cli.SIsMember(context.Background(), key, deviceID).Result()
}

//  del
func (rd redisDB) DelNodeWithCacheList(deviceID, cid string) error {
	key := fmt.Sprintf(redisKeyDataNodeList, cid)

	_, err := rd.cli.SRem(context.Background(), key, deviceID).Result()
	return err
}

func (rd redisDB) SetNodeInfo(deviceID string, info NodeInfo) error {
	key := fmt.Sprintf(redisKeyNodeInfo, deviceID)

	_, err := rd.cli.HMSet(context.Background(), key, lastTimeField, info.LastTime, geoField, info.Geo, isOnlineField, info.IsOnline).Result()
	if err != nil {
		return err
	}

	_, err = rd.cli.HIncrBy(context.Background(), key, onLineTimeField, info.OnLineTime).Result()
	return err
}

func (rd redisDB) GetNodeInfo(deviceID string) (NodeInfo, error) {
	key := fmt.Sprintf(redisKeyNodeInfo, deviceID)

	vals, err := rd.cli.HMGet(context.Background(), key, geoField, onLineTimeField, lastTimeField, isOnlineField).Result()
	if err != nil {
		return NodeInfo{}, err
	}

	if len(vals) <= 0 {
		return NodeInfo{}, xerrors.New("info not find")
	}

	// fmt.Printf("GetNodeInfo vals:%v", vals)

	if vals[0] == nil || vals[1] == nil || vals[2] == nil {
		return NodeInfo{}, xerrors.New("info not find")
	}

	g, _ := redigo.String(vals[0], nil)
	o, _ := redigo.Int64(vals[1], nil)
	l, _ := redigo.String(vals[2], nil)
	i, _ := redigo.Bool(vals[3], nil)

	return NodeInfo{Geo: g, OnLineTime: o, LastTime: l, IsOnline: i}, nil
}

//  add
func (rd redisDB) SetNodeToGeoList(deviceID, geo string) error {
	key := fmt.Sprintf(redisKeyGeoNodeList, geo)

	_, err := rd.cli.SAdd(context.Background(), key, deviceID).Result()
	return err
}

// SMembers
func (rd redisDB) GetNodesWithGeoList(geo string) ([]string, error) {
	key := fmt.Sprintf(redisKeyGeoNodeList, geo)

	return rd.cli.SMembers(context.Background(), key).Result()
}

//  del
func (rd redisDB) DelNodeWithGeoList(deviceID, geo string) error {
	key := fmt.Sprintf(redisKeyGeoNodeList, geo)

	_, err := rd.cli.SRem(context.Background(), key, deviceID).Result()
	return err
}

// node list: add
func (rd redisDB) SetNodeToNodeList(deviceID string, typeName api.NodeTypeName) error {
	key := fmt.Sprintf(redisKeyNodeList, typeName)

	_, err := rd.cli.SAdd(context.Background(), key, deviceID).Result()
	return err
}

//  node list: SMembers
func (rd redisDB) GetNodesWithNodeList(typeName api.NodeTypeName) ([]string, error) {
	key := fmt.Sprintf(redisKeyNodeList, typeName)

	return rd.cli.SMembers(context.Background(), key).Result()
}

// node list:   del
func (rd redisDB) DelNodeWithNodeList(deviceID string, typeName api.NodeTypeName) error {
	key := fmt.Sprintf(redisKeyNodeList, typeName)

	_, err := rd.cli.SRem(context.Background(), key, deviceID).Result()
	return err
}

//  add
func (rd redisDB) SetGeoToList(geo string) error {
	key := redisKeyGeoList

	_, err := rd.cli.SAdd(context.Background(), key, geo).Result()
	return err
}

// SMembers
func (rd redisDB) GetGeosWithList() ([]string, error) {
	key := redisKeyGeoList

	return rd.cli.SMembers(context.Background(), key).Result()
}

//  del
func (rd redisDB) DelGeoWithList(geo string) error {
	key := redisKeyGeoList

	_, err := rd.cli.SRem(context.Background(), key, geo).Result()
	return err
}

//  add
func (rd redisDB) SetValidatorToList(deviceID string) error {
	key := redisKeyValidatorList

	_, err := rd.cli.SAdd(context.Background(), key, deviceID).Result()
	return err
}

// SMembers
func (rd redisDB) GetValidatorsWithList() ([]string, error) {
	key := redisKeyValidatorList

	return rd.cli.SMembers(context.Background(), key).Result()
}

//  del
func (rd redisDB) DelValidatorList() error {
	key := redisKeyValidatorList

	_, err := rd.cli.Del(context.Background(), key).Result()
	return err
}

//  add
func (rd redisDB) SetGeoToValidatorList(deviceID, geo string) error {
	key := fmt.Sprintf(redisKeyValidatorGeoList, deviceID)

	_, err := rd.cli.SAdd(context.Background(), key, deviceID).Result()
	return err
}

// SMembers
func (rd redisDB) GetGeoWithValidatorList(deviceID string) ([]string, error) {
	key := fmt.Sprintf(redisKeyValidatorGeoList, deviceID)

	return rd.cli.SMembers(context.Background(), key).Result()
}

//  del
func (rd redisDB) DelValidatorGeoList(deviceID string) error {
	key := fmt.Sprintf(redisKeyValidatorGeoList, deviceID)

	_, err := rd.cli.Del(context.Background(), key).Result()
	return err
}
