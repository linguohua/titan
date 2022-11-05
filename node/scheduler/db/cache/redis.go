package cache

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"time"

	"github.com/linguohua/titan/api"

	"github.com/go-redis/redis/v8"
	redigo "github.com/gomodule/redigo/redis"
)

const (
	// redisKeyWaitingTask  server name
	redisKeyWaitingTask = "Titan:WaitingTask:%s"
	// redisKeyRunningList  server name
	redisKeyRunningList = "Titan:RunningList:%s"
	// redisKeyRunningTask  server name:cid
	redisKeyRunningTask = "Titan:RunningTask:%s:%s"
	// redisKeyCacheResult  server name
	redisKeyCacheResult = "Titan:CacheResult:%s"
	// RedisKeyNodeBlockFid  deviceID
	redisKeyNodeBlockFid = "Titan:NodeBlockFid:%s"
	// RedisKeyBlockNodeList  cid
	redisKeyBlockNodeList = "Titan:BlockNodeList:%s"
	// // RedisKeyGeoNodeList  geo
	// redisKeyGeoNodeList = "Titan:GeoNodeList:%s"
	// RedisKeyValidatorList server name
	redisKeyValidatorList = "Titan:ValidatorList:%s"
	// RedisKeyValidateRoundID server name
	redisKeyValidateRoundID = "Titan:ValidateRoundID:%s"
	// redisKeyValidateingList server name
	redisKeyValidateingList = "Titan:ValidateingList:%s"
	// // RedisKeyEdgeDeviceIDList
	// redisKeyEdgeDeviceIDList = "Titan:EdgeDeviceIDList"
	// // RedisKeyCandidateDeviceIDList
	// redisKeyCandidateDeviceIDList = "Titan:CandidateDeviceIDList"
	// RedisKeyNodeInfo  deviceID
	redisKeyNodeInfo = "Titan:NodeInfo:%s"
	// RedisKeyCacheID area
	redisKeyCacheID = "Titan:CacheID:%s"
	// redisKeyDataCacheKey
	redisKeyDataCacheKey = "Titan:DataCacheKey:%s"
	// RedisKeyCacheTask
	redisKeyCacheTask = "Titan:CacheTask"
	// RedisKeyNodeDeviceID
	redisKeyNodeDeviceID = "Titan:NodeDeviceID"
	// RedisKeyNodeReward
	redisKeyNodeDayReward = "Titan:NodeDayReward:%s"

	// NodeInfo field
	onlineTimeField         = "OnlineTime"
	validateSuccessField    = "ValidateSuccessTime"
	nodeTodayRewardField    = "TodayProfit"
	nodeRewardDateTimeField = "RewardDateTime"
	nodeLatencyField        = "Latency"
	// CacheTask field
	// carFileIDField = "CarFileID"
	// cacheIDField = "cacheID"
)

const (
	dayFormatLayout = "20060102"
	tebibyte        = 1024 * 1024 * 1024 * 1024
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

// func (rd redisDB) IncrDataCacheKey(cacheID string) (int64, error) {
// 	key := fmt.Sprintf(redisKeyDataCacheKey, cacheID)

// 	return rd.cli.IncrBy(context.Background(), key, 1).Result()
// }

// func (rd redisDB) DeleteDataCache(cacheID string) error {
// 	key := fmt.Sprintf(redisKeyDataCacheKey, cacheID)

// 	_, err := rd.cli.Del(context.Background(), key).Result()
// 	return err
// }

func (rd redisDB) IncrNodeOnlineTime(deviceID string, onlineTime float64) (float64, error) {
	key := fmt.Sprintf(redisKeyNodeInfo, deviceID)

	return rd.cli.HIncrByFloat(context.Background(), key, onlineTimeField, onlineTime).Result()
}

func (rd redisDB) IncrNodeValidateTime(deviceID string, validateSuccessTime int64) (int64, error) {
	key := fmt.Sprintf(redisKeyNodeInfo, deviceID)

	return rd.cli.HIncrBy(context.Background(), key, validateSuccessField, validateSuccessTime).Result()
}

// node cache tag ++1
func (rd redisDB) IncrCacheID(area string) (int64, error) {
	key := fmt.Sprintf(redisKeyCacheID, area)

	return rd.cli.IncrBy(context.Background(), key, 1).Result()
}

// node cache tag ++1
func (rd redisDB) IncrNodeCacheFid(deviceID string, num int) (int, error) {
	key := fmt.Sprintf(redisKeyNodeBlockFid, deviceID)
	n, err := rd.cli.IncrBy(context.Background(), key, int64(num)).Result()
	return int(n), err
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
	key := fmt.Sprintf(redisKeyValidateRoundID, serverName)

	return rd.cli.IncrBy(context.Background(), key, 1).Result()
}

// get ValidateID
func (rd redisDB) GetValidateRoundID() (string, error) {
	key := fmt.Sprintf(redisKeyValidateRoundID, serverName)

	return rd.cli.Get(context.Background(), key).Result()
}

// add
func (rd redisDB) SetNodeToValidateingList(deviceID string) error {
	key := fmt.Sprintf(redisKeyValidateingList, serverName)

	_, err := rd.cli.SAdd(context.Background(), key, deviceID).Result()
	return err
}

// SMembers
func (rd redisDB) GetNodesWithValidateingList() ([]string, error) {
	key := fmt.Sprintf(redisKeyValidateingList, serverName)

	return rd.cli.SMembers(context.Background(), key).Result()
}

// del device
func (rd redisDB) RemoveNodeWithValidateingList(deviceID string) error {
	key := fmt.Sprintf(redisKeyValidateingList, serverName)

	_, err := rd.cli.SRem(context.Background(), key, deviceID).Result()
	return err
}

// del key
func (rd redisDB) RemoveValidateingList() error {
	key := fmt.Sprintf(redisKeyValidateingList, serverName)

	_, err := rd.cli.Del(context.Background(), key).Result()
	return err
}

// add
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

// del
func (rd redisDB) RemoveNodeWithCacheList(deviceID, cid string) error {
	key := fmt.Sprintf(redisKeyBlockNodeList, cid)

	_, err := rd.cli.SRem(context.Background(), key, deviceID).Result()
	return err
}

// //  add
// func (rd redisDB) SetNodeToGeoList(deviceID, geo string) error {
// 	key := fmt.Sprintf(redisKeyGeoNodeList, geo)

// 	_, err := rd.cli.SAdd(context.Background(), key, deviceID).Result()
// 	return err
// }

// // SMembers
// func (rd redisDB) GetNodesWithGeoList(geo string) ([]string, error) {
// 	key := fmt.Sprintf(redisKeyGeoNodeList, geo)

// 	return rd.cli.SMembers(context.Background(), key).Result()
// }

// //  del
// func (rd redisDB) RemoveNodeWithGeoList(deviceID, geo string) error {
// 	key := fmt.Sprintf(redisKeyGeoNodeList, geo)

// 	_, err := rd.cli.SRem(context.Background(), key, deviceID).Result()
// 	return err
// }

// add
func (rd redisDB) SetValidatorToList(deviceID string) error {
	key := fmt.Sprintf(redisKeyValidatorList, serverName)

	_, err := rd.cli.SAdd(context.Background(), key, deviceID).Result()
	return err
}

// SMembers
func (rd redisDB) GetValidatorsWithList() ([]string, error) {
	key := fmt.Sprintf(redisKeyValidatorList, serverName)

	return rd.cli.SMembers(context.Background(), key).Result()
}

// del
func (rd redisDB) RemoveValidatorList() error {
	key := fmt.Sprintf(redisKeyValidatorList, serverName)

	_, err := rd.cli.Del(context.Background(), key).Result()
	return err
}

// SISMEMBER
func (rd redisDB) IsNodeInValidatorList(deviceID string) (bool, error) {
	key := fmt.Sprintf(redisKeyValidatorList, serverName)

	return rd.cli.SIsMember(context.Background(), key, deviceID).Result()
}

// //  add
// func (rd redisDB) SetEdgeDeviceIDList(deviceIDs []string) error {
// 	_, err := rd.cli.SAdd(context.Background(), redisKeyEdgeDeviceIDList, deviceIDs).Result()
// 	return err
// }

// // SISMEMBER
// func (rd redisDB) IsEdgeInDeviceIDList(deviceID string) (bool, error) {
// 	return rd.cli.SIsMember(context.Background(), redisKeyEdgeDeviceIDList, deviceID).Result()
// }

// //  add
// func (rd redisDB) SetCandidateDeviceIDList(deviceIDs []string) error {
// 	_, err := rd.cli.SAdd(context.Background(), redisKeyCandidateDeviceIDList, deviceIDs).Result()
// 	return err
// }

// // SISMEMBER
// func (rd redisDB) IsCandidateInDeviceIDList(deviceID string) (bool, error) {
// 	return rd.cli.SIsMember(context.Background(), redisKeyCandidateDeviceIDList, deviceID).Result()
// }

// func (rd redisDB) SetCacheDataTask(cid, cacheID string) error {
// 	rd.cli.Expire(context.Background(), redisKeyCacheTask, time.Second*20)

// 	_, err := rd.cli.HMSet(context.Background(), redisKeyCacheTask, carFileIDField, cid, cacheIDField, cacheID).Result()
// 	return err
// }

// // SISMEMBER
// func (rd redisDB) RemoveCacheDataTask() error {
// 	_, err := rd.cli.Del(context.Background(), redisKeyCacheTask).Result()
// 	return err
// }

// // SISMEMBER
// func (rd redisDB) GetCacheDataTask() (string, string) {
// 	vals, err := rd.cli.HMGet(context.Background(), redisKeyCacheTask, carFileIDField, cacheIDField).Result()
// 	if err != nil || vals == nil || len(vals) <= 0 {
// 		return "", ""
// 	}

// 	cid, _ := redigo.String(vals[0], nil)
// 	cacheID, _ := redigo.String(vals[1], nil)
// 	return cid, cacheID
// }

func (rd redisDB) IncrDeviceReward(deviceID string, reward int64) error {
	key := fmt.Sprintf(redisKeyNodeInfo, deviceID)

	results, err := rd.cli.HMGet(context.Background(), key, nodeTodayRewardField, nodeRewardDateTimeField).Result()
	if err != nil {
		return err
	}

	var (
		datetime     time.Time
		beforeReward int64
	)

	if resReward, ok := results[0].(string); ok {
		beforeReward, err = strconv.ParseInt(resReward, 10, 64)
		if err != nil {
			return err
		}
	}

	if date, ok := results[1].(string); ok {
		datetime, err = time.Parse(dayFormatLayout, date)
		if err != nil {
			return err
		}
	}

	if getStartOfDay(datetime).Equal(getStartOfDay(time.Now())) {
		reward += beforeReward
	}

	_, err = rd.cli.HMSet(context.Background(), key,
		nodeTodayRewardField, reward,
		nodeRewardDateTimeField, getStartOfDay(time.Now()).Format(dayFormatLayout),
	).Result()
	if err != nil {
		return err
	}

	return nil
}

func getStartOfDay(t time.Time) time.Time {
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.Local)
}

func (rd redisDB) SetDeviceInfo(deviceID string, info api.DevicesInfo) (bool, error) {
	key := fmt.Sprintf(redisKeyNodeInfo, deviceID)

	ctx := context.Background()
	_, err := rd.cli.Pipelined(ctx, func(pipeliner redis.Pipeliner) error {
		for field, value := range toMap(info) {
			if field == nodeTodayRewardField || field == onlineTimeField {
				continue
			}
			pipeliner.HMSet(ctx, key, field, value)
		}
		return nil
	})
	if err != nil {
		return false, err
	}

	return true, nil
}

func toMap(info api.DevicesInfo) map[string]interface{} {
	out := make(map[string]interface{})
	t := reflect.TypeOf(info)
	v := reflect.ValueOf(info)
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		redisTag := field.Tag.Get("redis")
		if redisTag == "" {
			continue
		}
		out[redisTag] = v.Field(i).Interface()
	}
	return out
}

func (rd redisDB) GetDeviceInfo(deviceID string) (api.DevicesInfo, error) {
	key := fmt.Sprintf(redisKeyNodeInfo, deviceID)

	var info api.DevicesInfo
	err := rd.cli.HGetAll(context.Background(), key).Scan(&info)
	if err != nil {
		return api.DevicesInfo{}, err
	}

	return info, nil
}

func (rd redisDB) SetCacheResultInfo(info api.CacheResultInfo) error {
	key := fmt.Sprintf(redisKeyCacheResult, serverName)

	bytes, err := json.Marshal(info)
	if err != nil {
		return err
	}

	_, err = rd.cli.RPush(context.Background(), key, bytes).Result()
	return err
}

func (rd redisDB) GetCacheResultInfo() (api.CacheResultInfo, error) {
	key := fmt.Sprintf(redisKeyCacheResult, serverName)

	value, err := rd.cli.LIndex(context.Background(), key, 0).Result()

	var info api.CacheResultInfo
	bytes, err := redigo.Bytes(value, nil)
	if err != nil {
		return api.CacheResultInfo{}, err
	}

	if err := json.Unmarshal(bytes, &info); err != nil {
		return api.CacheResultInfo{}, err
	}

	return info, nil
}

func (rd redisDB) GetCacheResultNum() int64 {
	key := fmt.Sprintf(redisKeyCacheResult, serverName)

	l, _ := rd.cli.LLen(context.Background(), key).Result()
	return l
}

func (rd redisDB) RemoveCacheResultInfo() error {
	key := fmt.Sprintf(redisKeyCacheResult, serverName)

	_, err := rd.cli.LPop(context.Background(), key).Result()
	return err
}

func (rd redisDB) SetRunningCacheTask(cid, cacheID string) error {
	key := fmt.Sprintf(redisKeyRunningTask, serverName, cid)
	// Expire
	_, err := rd.cli.Set(context.Background(), key, cacheID, time.Second*20).Result()
	return err
}

// func (rd redisDB) GetRunningCacheTask(cid string) (int64, error) {
// 	key := fmt.Sprintf(redisKeyRunningTask, serverName, cid)

// 	return rd.cli.Exists(context.Background(), key).Result()
// }

func (rd redisDB) GetRunningCacheTask(cid string) (string, error) {
	key := fmt.Sprintf(redisKeyRunningTask, serverName, cid)

	return rd.cli.Get(context.Background(), key).Result()
}

func (rd redisDB) RemoveRunningCacheTask(cid, cacheID string) error {
	key := fmt.Sprintf(redisKeyRunningTask, serverName, cid)

	_, err := rd.cli.Del(context.Background(), key).Result()
	if err != nil {
		return err
	}

	return rd.RemoveRunningList(cid, cacheID)
}

func (rd redisDB) SetWaitingCacheTask(info api.CacheDataInfo) error {
	key := fmt.Sprintf(redisKeyWaitingTask, serverName)

	bytes, err := json.Marshal(info)
	if err != nil {
		return err
	}

	_, err = rd.cli.RPush(context.Background(), key, bytes).Result()
	return err
}

func (rd redisDB) GetWaitingCacheTask() (api.CacheDataInfo, error) {
	key := fmt.Sprintf(redisKeyWaitingTask, serverName)

	value, err := rd.cli.LIndex(context.Background(), key, 0).Result()

	if value == "" {
		return api.CacheDataInfo{}, redis.Nil
	}

	var info api.CacheDataInfo
	bytes, err := redigo.Bytes(value, nil)
	if err != nil {
		return api.CacheDataInfo{}, err
	}

	if err := json.Unmarshal(bytes, &info); err != nil {
		return api.CacheDataInfo{}, err
	}

	return info, nil
}

func (rd redisDB) RemoveWaitingCacheTask() error {
	key := fmt.Sprintf(redisKeyWaitingTask, serverName)

	_, err := rd.cli.LPop(context.Background(), key).Result()
	return err
}

// add
func (rd redisDB) SetCidToRunningList(cid, cacheID string) error {
	key := fmt.Sprintf(redisKeyRunningList, serverName)

	cKey := fmt.Sprintf("%s:%s", cid, cacheID)
	_, err := rd.cli.SAdd(context.Background(), key, cKey).Result()
	return err
}

// del
func (rd redisDB) RemoveRunningList(cid, cacheID string) error {
	key := fmt.Sprintf(redisKeyRunningList, serverName)

	cKey := fmt.Sprintf("%s:%s", cid, cacheID)
	_, err := rd.cli.SRem(context.Background(), key, cKey).Result()
	return err
}

// SMembers
func (rd redisDB) GetTasksWithRunningList() ([]string, error) {
	key := fmt.Sprintf(redisKeyRunningList, serverName)

	return rd.cli.SMembers(context.Background(), key).Result()
}

// // SISMEMBER
// func (rd redisDB) IsCidInRunningList(cid string) (bool, error) {
// 	key := fmt.Sprintf(redisKeyRunningTask, serverName)

// 	return rd.cli.SIsMember(context.Background(), key, cid).Result()
// }

func (rd redisDB) SetDeviceLatency(deviceID string, latency float64) error {
	key := fmt.Sprintf(redisKeyNodeInfo, deviceID)
	_, err := rd.cli.HMSet(context.Background(), key, nodeLatencyField, latency).Result()
	if err != nil {
		return err
	}
	return nil
}

func (rd redisDB) GetDeviceStat() (out api.StateNetwork, err error) {
	ctx := context.Background()
	keys, err := rd.cli.Keys(ctx, fmt.Sprintf(redisKeyNodeInfo, "*")).Result()
	if err != nil {
		return
	}

	for _, key := range keys {
		node := api.DevicesInfo{}
		if err = rd.cli.HGetAll(ctx, key).Scan(&node); err != nil {
			continue
		}

		switch node.NodeType {
		case api.NodeCandidate:
			out.AllCandidate++
		case api.NodeEdge:
			out.AllEdgeNode++
		default:
			continue
		}

		out.StorageT += node.DiskSpace / float64(tebibyte)
		out.TotalBandwidthDown += node.BandwidthDown
		out.TotalBandwidthUp += node.BandwidthUp
	}

	return
}
