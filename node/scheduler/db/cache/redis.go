package cache

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
	// RedisKeyNodeBlockCids  deviceID
	redisKeyNodeBlockCids = "Titan:NodeBlockCids:%s"
	// RedisKeyNodeBlockFids  deviceID
	redisKeyNodeBlockFids = "Titan:NodeBlockFids:%s"
	// RedisKeyNodeBlockFid  deviceID
	redisKeyNodeBlockFid = "Titan:NodeBlockFid:%s"
	// RedisKeyNodeFailBlocks  deviceID
	// redisKeyNodeFailBlocks = "Titan:NodeFailBlocks:%s"
	// RedisKeyBlockNodeList  cid
	redisKeyBlockNodeList = "Titan:BlockNodeList:%s"
	// RedisKeyGeoNodeList  geo
	redisKeyGeoNodeList = "Titan:GeoNodeList:%s"
	// RedisKeyGeoList
	redisKeyGeoList = "Titan:GeoList"
	// RedisKeyValidatorList
	redisKeyValidatorList = "Titan:ValidatorList"
	// RedisKeyValidatorGeoList deviceID
	redisKeyValidatorGeoList = "Titan:ValidatorGeoList:%s"
	// RedisKeyValidateRoundID
	redisKeyValidateRoundID = "Titan:ValidateRoundID"
	// RedisKeyValidateResult ValidateID:edgeID
	redisKeyValidateResult = "Titan:ValidateResult:%s:%s"
	// redisKeyValidateErrorList ValidateID
	redisKeyValidateErrorList = "Titan:ValidateErrorList:%s"
	// redisKeyValidateingList
	redisKeyValidateingList = "Titan:ValidateingList"

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

	// Validate field
	stratTimeField = "StratTime"
	endTimeField   = "EndTime"
	validatorField = "Validator"
	statusField    = "Status"
	msgField       = "Msg"
)

// TypeRedis redis
func TypeRedis() string {
	return "Redis"
}

type redisDB struct {
	cli *redis.Client
}

// InitRedis init redis pool
func InitRedis(url string) (DB, error) {
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

// IsNilErr Is NilErr
func (rd redisDB) IsNilErr(err error) bool {
	return errors.Is(err, redis.Nil)
}

// node cache tag ++1
func (rd redisDB) IncrNodeCacheFid(deviceID string) (int64, error) {
	key := fmt.Sprintf(redisKeyNodeBlockFid, deviceID)
	return rd.cli.IncrBy(context.Background(), key, 1).Result()
}

func (rd redisDB) GetNodeCacheFid(deviceID string) (int64, error) {
	key := fmt.Sprintf(redisKeyNodeBlockFid, deviceID)
	val, err := rd.cli.Get(context.Background(), key).Result()
	if err != nil {
		return 0, err
	}
	return strconv.ParseInt(val, 10, 64)
}

// ValidateID ++1
func (rd redisDB) IncrValidateRoundID() (int64, error) {
	return rd.cli.IncrBy(context.Background(), redisKeyValidateRoundID, 1).Result()
}

// get ValidateID
func (rd redisDB) GetValidateRoundID() (string, error) {
	return rd.cli.Get(context.Background(), redisKeyValidateRoundID).Result()
}

// del node data with cid
func (rd redisDB) RemoveBlockFidWithCid(deviceID, cid string) error {
	key := fmt.Sprintf(redisKeyNodeBlockCids, deviceID)

	_, err := rd.cli.HDel(context.Background(), key, cid).Result()
	// _, err := rd.cli.ZRem(context.Background(), key, cid).Result()
	return err
}

// set cid
func (rd redisDB) SetBlockFidWithCid(deviceID, cid string, fid string) error {
	key := fmt.Sprintf(redisKeyNodeBlockCids, deviceID)

	_, err := rd.cli.HSet(context.Background(), key, cid, fid).Result()
	// _, err := rd.cli.ZAdd(context.Background(), key, &redis.Z{Score: 1, Member: cid}).Result()
	return err
}

// get cache info
func (rd redisDB) GetBlockFidWithCid(deviceID, cid string) (string, error) {
	key := fmt.Sprintf(redisKeyNodeBlockCids, deviceID)

	return rd.cli.HGet(context.Background(), key, cid).Result()
	// return rd.cli.ZRank(context.Background(), key, cid).Result()
}

func (rd redisDB) GetBlockCidNum(deviceID string) (int64, error) {
	key := fmt.Sprintf(redisKeyNodeBlockCids, deviceID)

	// return rd.cli.ZCard(context.Background(), key).Result()
	return rd.cli.HLen(context.Background(), key).Result()
}

// get all cache info
func (rd redisDB) GetBlockCids(deviceID string) (map[string]string, error) {
	// func (rd redisDB) GetCacheBlockInfos(deviceID string, start, end int64) ([]string, error) {
	key := fmt.Sprintf(redisKeyNodeBlockCids, deviceID)
	return rd.cli.HGetAll(context.Background(), key).Result()
	// return rd.cli.ZRange(context.Background(), key, start, end).Result()
}

func (rd redisDB) RemoveBlockCidWithFid(deviceID, fid string) error {
	key := fmt.Sprintf(redisKeyNodeBlockFids, deviceID)

	_, err := rd.cli.HDel(context.Background(), key, fid).Result()
	return err
}

func (rd redisDB) SetBlockCidWithFid(deviceID, cid string, fid string) error {
	key := fmt.Sprintf(redisKeyNodeBlockFids, deviceID)

	_, err := rd.cli.HSet(context.Background(), key, fid, cid).Result()
	return err
}

func (rd redisDB) GetBlockCidWithFid(deviceID, fid string) (string, error) {
	key := fmt.Sprintf(redisKeyNodeBlockFids, deviceID)

	str, err := rd.cli.HGet(context.Background(), key, fid).Result()
	if err != nil {
		return "", err
	}

	return str, nil
}

func (rd redisDB) GetBlockFidNum(deviceID string) (int64, error) {
	key := fmt.Sprintf(redisKeyNodeBlockFids, deviceID)

	// return rd.cli.ZCard(context.Background(), key).Result()
	return rd.cli.HLen(context.Background(), key).Result()
}

func (rd redisDB) GetBlockFids(deviceID string) (map[string]string, error) {
	key := fmt.Sprintf(redisKeyNodeBlockFids, deviceID)
	return rd.cli.HGetAll(context.Background(), key).Result()
}

//  add
func (rd redisDB) SetNodeToValidateingList(deviceID string) error {
	_, err := rd.cli.SAdd(context.Background(), redisKeyValidateingList, deviceID).Result()
	return err
}

// SMembers
func (rd redisDB) GetNodesWithValidateingList() ([]string, error) {
	return rd.cli.SMembers(context.Background(), redisKeyValidateingList).Result()
}

//  del device
func (rd redisDB) RemoveNodeWithValidateingList(deviceID string) error {
	_, err := rd.cli.SRem(context.Background(), redisKeyValidateingList, deviceID).Result()
	return err
}

//  del key
func (rd redisDB) RemoveValidateingList() error {
	_, err := rd.cli.Del(context.Background(), redisKeyValidateingList).Result()
	return err
}

//  add
func (rd redisDB) SetNodeToCacheList(deviceID, cid string) error {
	key := fmt.Sprintf(redisKeyBlockNodeList, cid)

	_, err := rd.cli.SAdd(context.Background(), key, deviceID).Result()
	return err
}

// SMembers
func (rd redisDB) GetNodesWithCacheList(cid string) ([]string, error) {
	key := fmt.Sprintf(redisKeyBlockNodeList, cid)

	return rd.cli.SMembers(context.Background(), key).Result()
}

// SISMEMBER
func (rd redisDB) IsNodeInCacheList(cid, deviceID string) (bool, error) {
	key := fmt.Sprintf(redisKeyBlockNodeList, cid)

	return rd.cli.SIsMember(context.Background(), key, deviceID).Result()
}

//  del
func (rd redisDB) RemoveNodeWithCacheList(deviceID, cid string) error {
	key := fmt.Sprintf(redisKeyBlockNodeList, cid)

	_, err := rd.cli.SRem(context.Background(), key, deviceID).Result()
	return err
}

//  add
// func (rd redisDB) SetBlockToNodeFailList(deviceID, cid string) error {
// 	key := fmt.Sprintf(redisKeyNodeFailBlocks, deviceID)

// 	_, err := rd.cli.SAdd(context.Background(), key, cid).Result()
// 	return err
// }

// // SMembers
// func (rd redisDB) GetBlocksWithNodeFailList(deviceID string) ([]string, error) {
// 	key := fmt.Sprintf(redisKeyNodeFailBlocks, deviceID)

// 	return rd.cli.SMembers(context.Background(), key).Result()
// }

// //  del
// func (rd redisDB) RemoveBlockWithNodeFailList(deviceID, cid string) error {
// 	key := fmt.Sprintf(redisKeyNodeFailBlocks, deviceID)

// 	_, err := rd.cli.SRem(context.Background(), key, cid).Result()
// 	return err
// }

func (rd redisDB) SetNodeInfo(deviceID string, info *NodeInfo) error {
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

func (rd redisDB) GetNodeInfo(deviceID string) (*NodeInfo, error) {
	key := fmt.Sprintf(redisKeyNodeInfo, deviceID)

	vals, err := rd.cli.HMGet(context.Background(), key, geoField, onLineTimeField, lastTimeField, isOnlineField, nodeType).Result()
	if err != nil {
		return nil, err
	}

	if len(vals) <= 0 {
		return nil, xerrors.New(NotFind)
	}

	// fmt.Printf("GetNodeInfo vals:%v", vals)

	if vals[0] == nil || vals[1] == nil || vals[2] == nil || vals[3] == nil || vals[4] == nil {
		return nil, xerrors.New(NotFind)
	}

	g, _ := redigo.String(vals[0], nil)
	o, _ := redigo.Int64(vals[1], nil)
	l, _ := redigo.String(vals[2], nil)
	i, _ := redigo.Bool(vals[3], nil)
	n, _ := redigo.String(vals[4], nil)

	return &NodeInfo{Geo: g, OnLineTime: o, LastTime: l, IsOnline: i, NodeType: api.NodeTypeName(n)}, nil
}

func (rd redisDB) SetValidateResultInfo(sID string, edgeID, validator, msg string, status ValidateStatus) error {
	key := fmt.Sprintf(redisKeyValidateResult, sID, edgeID)

	nowTime := time.Now().Format("2006-01-02 15:04:05")

	if status == ValidateStatusCreate {
		_, err := rd.cli.HMSet(context.Background(), key, validatorField, validator, stratTimeField, nowTime, statusField, int(status)).Result()
		return err

	} else if status > ValidateStatusCreate {
		_, err := rd.cli.HMSet(context.Background(), key, endTimeField, nowTime, statusField, int(status), msgField, msg).Result()
		return err
	}

	return xerrors.Errorf("SetValidateResultInfo status:%v", status)
}

func (rd redisDB) SetNodeToValidateErrorList(sID string, deviceID string) error {
	key := fmt.Sprintf(redisKeyValidateErrorList, sID)

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
func (rd redisDB) RemoveNodeWithGeoList(deviceID, geo string) error {
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
// func (rd redisDB) RemoveNodeWithNodeList(deviceID string, typeName api.NodeTypeName) error {
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
func (rd redisDB) RemoveGeoWithList(geo string) error {
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
func (rd redisDB) RemoveValidatorList() error {
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
func (rd redisDB) RemoveValidatorGeoList(deviceID string) error {
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
