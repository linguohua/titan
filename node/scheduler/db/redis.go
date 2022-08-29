package db

import (
	"context"
	"errors"
	"fmt"
	"strconv"
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
	// RedisKeyNodeDataTags  deviceID
	redisKeyNodeDataTags = "Titan:NodeDataTags:%s"
	// RedisKeyDataNodeList  cid
	redisKeyDataNodeList = "Titan:DataNodeList:%s"
	// RedisKeyNodeDataTag  deviceID
	redisKeyNodeDataTag = "Titan:NodeDataTag:%s"
	// RedisKeyGeoNodeList  geo
	redisKeyGeoNodeList = "Titan:GeoNodeList:%s"
	// RedisKeyNodeList  Edge/Candidate
	// redisKeyNodeList = "Titan:NodeList:%s"
	// RedisKeyGeoList
	redisKeyGeoList = "Titan:GeoList"
	// RedisKeyValidatorList
	redisKeyValidatorList = "Titan:ValidatorList"
	// RedisKeyValidatorGeoList deviceID
	redisKeyValidatorGeoList = "Titan:ValidatorGeoList:%s"
	// RedisKeyVerifyID
	redisKeyVerifyID = "Titan:VerifyID"
	// RedisKeyVerifyResult VerifyID:edgeID
	redisKeyVerifyResult = "Titan:VerifyResult:%s:%s"
	// RedisKeyVerifyOfflineList VerifyID
	redisKeyVerifyOfflineList = "Titan:VerifyOfflineList:%s"
	// redisKeyVerifyList 用于检查超时
	redisKeyVerifyList = "Titan:VerifyList"

	// RedisKeyEdgeDeviceIDList
	redisKeyEdgeDeviceIDList = "Titan:EdgeDeviceIDList"
	// RedisKeyCandidateDeviceIDList
	redisKeyCandidateDeviceIDList = "Titan:CandidateDeviceIDList"

	// node info field
	lastTimeField   = "LastTime"
	onLineTimeField = "OnLineTime"
	geoField        = "Geo"
	isOnlineField   = "IsOnline"
	nodeType        = "NodeType"

	// Verify field
	stratTimeField = "StratTime"
	endTimeField   = "EndTime"
	validatorField = "Validator"
	statusField    = "Status"
	msgField       = "Msg"
)

type redisDB struct {
	cli *redis.Client
}

// TypeRedis redis
func TypeRedis() string {
	return "Redis"
}

// IsNilErr Is NilErr
func (rd redisDB) IsNilErr(err error) bool {
	return errors.Is(err, redis.Nil)
}

// InitRedis init redis pool
func InitRedis(url string) (CacheDB, error) {
	// fmt.Printf("redis init url:%v", url)

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

// node cache tag ++1
func (rd redisDB) IncrNodeCacheTag(deviceID string) (int64, error) {
	key := fmt.Sprintf(redisKeyNodeDataTag, deviceID)

	return rd.cli.IncrBy(context.Background(), key, 1).Result()
}

// get VerifyID
func (rd redisDB) GetNodeCacheTag(deviceID string) (int64, error) {
	key := fmt.Sprintf(redisKeyNodeDataTag, deviceID)

	val, err := rd.cli.Get(context.Background(), key).Result()
	if err != nil {
		return 0, err
	}

	return strconv.ParseInt(val, 10, 64)
}

// VerifyID ++1
func (rd redisDB) IncrVerifyID() (int64, error) {
	return rd.cli.IncrBy(context.Background(), redisKeyVerifyID, 1).Result()
}

// get VerifyID
func (rd redisDB) GetVerifyID() (string, error) {
	return rd.cli.Get(context.Background(), redisKeyVerifyID).Result()
}

// del node data with cid
func (rd redisDB) DelCacheDataInfo(deviceID, cid string) error {
	key := fmt.Sprintf(redisKeyNodeDatas, deviceID)

	_, err := rd.cli.HDel(context.Background(), key, cid).Result()
	return err
}

// set cid
func (rd redisDB) SetCacheDataInfo(deviceID, cid string, tag string) error {
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

// del node data with cid
func (rd redisDB) DelCacheDataTagInfo(deviceID, tag string) error {
	key := fmt.Sprintf(redisKeyNodeDataTags, deviceID)

	_, err := rd.cli.HDel(context.Background(), key, tag).Result()
	return err
}

// set cid
func (rd redisDB) SetCacheDataTagInfo(deviceID, cid string, tag string) error {
	key := fmt.Sprintf(redisKeyNodeDataTags, deviceID)

	_, err := rd.cli.HSet(context.Background(), key, tag, cid).Result()
	return err
}

// get cache info
func (rd redisDB) GetCacheDataTagInfo(deviceID, tag string) (string, error) {
	key := fmt.Sprintf(redisKeyNodeDataTags, deviceID)

	str, err := rd.cli.HGet(context.Background(), key, tag).Result()
	if err != nil {
		return "", err
	}

	return str, nil
}

// get all cache info
func (rd redisDB) GetCacheDataTagInfos(deviceID string) (map[string]string, error) {
	key := fmt.Sprintf(redisKeyNodeDataTags, deviceID)

	return rd.cli.HGetAll(context.Background(), key).Result()
}

//  add
func (rd redisDB) SetNodeToVerifyList(deviceID string) error {
	_, err := rd.cli.SAdd(context.Background(), redisKeyVerifyList, deviceID).Result()
	return err
}

// SMembers
func (rd redisDB) GetNodesWithVerifyList() ([]string, error) {
	return rd.cli.SMembers(context.Background(), redisKeyVerifyList).Result()
}

//  del device
func (rd redisDB) DelNodeWithVerifyList(deviceID string) error {
	_, err := rd.cli.SRem(context.Background(), redisKeyVerifyList, deviceID).Result()
	return err
}

//  del key
func (rd redisDB) DelVerifyList() error {
	_, err := rd.cli.Del(context.Background(), redisKeyVerifyList).Result()
	return err
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

	nType := string(info.NodeType)

	_, err := rd.cli.HMSet(context.Background(), key, lastTimeField, info.LastTime, geoField, info.Geo, isOnlineField, info.IsOnline, nodeType, nType).Result()
	return err
}

func (rd redisDB) AddNodeOnlineTime(deviceID string, onLineTime int64) error {
	key := fmt.Sprintf(redisKeyNodeInfo, deviceID)

	_, err := rd.cli.HIncrBy(context.Background(), key, onLineTimeField, onLineTime).Result()
	return err
}

func (rd redisDB) GetNodeInfo(deviceID string) (NodeInfo, error) {
	key := fmt.Sprintf(redisKeyNodeInfo, deviceID)

	vals, err := rd.cli.HMGet(context.Background(), key, geoField, onLineTimeField, lastTimeField, isOnlineField, nodeType).Result()
	if err != nil {
		return NodeInfo{}, err
	}

	if len(vals) <= 0 {
		return NodeInfo{}, xerrors.New(NotFind)
	}

	// fmt.Printf("GetNodeInfo vals:%v", vals)

	if vals[0] == nil || vals[1] == nil || vals[2] == nil || vals[3] == nil || vals[4] == nil {
		return NodeInfo{}, xerrors.New(NotFind)
	}

	g, _ := redigo.String(vals[0], nil)
	o, _ := redigo.Int64(vals[1], nil)
	l, _ := redigo.String(vals[2], nil)
	i, _ := redigo.Bool(vals[3], nil)
	n, _ := redigo.String(vals[4], nil)

	return NodeInfo{Geo: g, OnLineTime: o, LastTime: l, IsOnline: i, NodeType: api.NodeTypeName(n)}, nil
}

func (rd redisDB) SetVerifyResultInfo(sID string, edgeID, validator, msg string, status VerifyStatus) error {
	key := fmt.Sprintf(redisKeyVerifyResult, sID, edgeID)

	nowTime := time.Now().Format("2006-01-02 15:04:05")

	if status == VerifyStatusCreate {
		_, err := rd.cli.HMSet(context.Background(), key, validatorField, validator, stratTimeField, nowTime, statusField, int(status)).Result()
		return err

	} else if status > VerifyStatusCreate {
		_, err := rd.cli.HMSet(context.Background(), key, endTimeField, nowTime, statusField, int(status), msgField, msg).Result()
		return err
	}

	return xerrors.Errorf("SetVerifyResultInfo status:%v", status)
}

func (rd redisDB) SetNodeToVerifyOfflineList(sID string, deviceID string) error {
	key := fmt.Sprintf(redisKeyVerifyOfflineList, sID)

	_, err := rd.cli.SAdd(context.Background(), key, deviceID).Result()
	return err
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

// // node list: add
// func (rd redisDB) SetNodeToNodeList(deviceID string, typeName api.NodeTypeName) error {
// 	key := fmt.Sprintf(redisKeyNodeList, typeName)

// 	_, err := rd.cli.SAdd(context.Background(), key, deviceID).Result()
// 	return err
// }

// //  node list: SMembers
// func (rd redisDB) GetNodesWithNodeList(typeName api.NodeTypeName) ([]string, error) {
// 	key := fmt.Sprintf(redisKeyNodeList, typeName)

// 	return rd.cli.SMembers(context.Background(), key).Result()
// }

// // node list:   del
// func (rd redisDB) DelNodeWithNodeList(deviceID string, typeName api.NodeTypeName) error {
// 	key := fmt.Sprintf(redisKeyNodeList, typeName)

// 	_, err := rd.cli.SRem(context.Background(), key, deviceID).Result()
// 	return err
// }

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

// SISMEMBER
func (rd redisDB) IsNodeInValidatorList(deviceID string) (bool, error) {
	key := redisKeyValidatorList

	return rd.cli.SIsMember(context.Background(), key, deviceID).Result()
}

//  add
func (rd redisDB) SetGeoToValidatorList(deviceID, geo string) error {
	key := fmt.Sprintf(redisKeyValidatorGeoList, deviceID)

	_, err := rd.cli.SAdd(context.Background(), key, geo).Result()
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

//  add
func (rd redisDB) SetEdgeDeviceIDList(deviceIDs []string) error {
	_, err := rd.cli.SAdd(context.Background(), redisKeyEdgeDeviceIDList, deviceIDs).Result()
	return err
}

// SISMEMBER
func (rd redisDB) IsEdgeInDeviceIDList(deviceID string) (bool, error) {
	return rd.cli.SIsMember(context.Background(), redisKeyEdgeDeviceIDList, deviceID).Result()
}

//  add
func (rd redisDB) SetCandidateDeviceIDList(deviceIDs []string) error {
	_, err := rd.cli.SAdd(context.Background(), redisKeyCandidateDeviceIDList, deviceIDs).Result()
	return err
}

// SISMEMBER
func (rd redisDB) IsCandidateInDeviceIDList(deviceID string) (bool, error) {
	return rd.cli.SIsMember(context.Background(), redisKeyCandidateDeviceIDList, deviceID).Result()
}
