package cache

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
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
	onlineTimeField      = "OnlineTime"
	validateSuccessField = "ValidateSuccessTime"
	deviceInfoField      = "DeviceInfo"
	// CacheTask field
	carFileIDField = "CarFileID"
	cacheIDField   = "cacheID"
)

const dayFormatLayout = "20060102"

var (
	hoursOfDay    = 24
	daysOfWeek    = 7
	daysOfMonth   = 30
	weekDuration  = time.Duration(daysOfWeek*hoursOfDay) * time.Hour
	monthDuration = time.Duration(daysOfMonth*hoursOfDay) * time.Hour
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

func (rd redisDB) IncrNodeReward(deviceID string, reward int64) error {
	key := fmt.Sprintf(redisKeyNodeDayReward, deviceID)
	dayField := time.Now().Format(dayFormatLayout)

	exist, err := rd.cli.HExists(context.Background(), key, dayField).Result()
	if err != nil {
		return err
	}

	if exist {
		_, err = rd.cli.HIncrBy(context.Background(), key, dayField, reward).Result()
		return err
	}

	keys, err := rd.cli.HKeys(context.Background(), key).Result()
	if err != nil {
		return err
	}

	if len(keys) > daysOfMonth {
		sort.Slice(keys, func(i, j int) bool { return keys[i] > keys[j] })
		_, err := rd.cli.HDel(context.Background(), key, keys[len(keys)-1]).Result()
		if err != nil {
			return err
		}
	}

	_, err = rd.cli.HIncrBy(context.Background(), key, dayField, reward).Result()
	return err
}

func (rd redisDB) GetNodeReward(deviceID string) (rewardInDay, rewardInWeek, rewardInMonth int64, err error) {
	key := fmt.Sprintf(redisKeyNodeDayReward, deviceID)

	res, err := rd.cli.HGetAll(context.Background(), key).Result()
	if err != nil {
		return 0, 0, 0, err
	}

	keys, err := rd.cli.HKeys(context.Background(), key).Result()
	if err != nil {
		return 0, 0, 0, err
	}

	sort.Slice(keys, func(i, j int) bool { return keys[i] > keys[j] })

	for _, k := range keys {
		val, ok := res[k]
		if !ok {
			continue
		}

		reward, err := strconv.ParseInt(val, 10, 64)
		if err != nil {
			return 0, 0, 0, err
		}

		t, err := time.Parse(dayFormatLayout, k)
		if err != nil {
			return 0, 0, 0, err
		}

		if getStartOfDay(t).Equal(getStartOfDay(time.Now())) {
			rewardInDay = reward
		}

		if getStartOfDay(t).After(getStartOfDay(time.Now().Add(-weekDuration))) {
			rewardInWeek += reward
		}

		if getStartOfDay(t).After(getStartOfDay(time.Now().Add(-monthDuration))) {
			rewardInMonth += reward
		}
	}

	return
}

func getStartOfDay(t time.Time) time.Time {
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.Local)
}

func (rd redisDB) SetDeviceInfo(deviceID string, info api.DevicesInfo) (bool, error) {
	key := fmt.Sprintf(redisKeyNodeInfo, deviceID)

	bytes, err := json.Marshal(info)
	if err != nil {
		return false, err
	}

	return rd.cli.HMSet(context.Background(), key, deviceInfoField, bytes).Result()
}

func (rd redisDB) GetDeviceInfo(deviceID string) (api.DevicesInfo, error) {
	key := fmt.Sprintf(redisKeyNodeInfo, deviceID)

	values, err := rd.cli.HMGet(context.Background(), key, deviceInfoField, onlineTimeField).Result()
	if err != nil {
		return api.DevicesInfo{}, err
	}

	var info api.DevicesInfo
	bytes, err := redigo.Bytes(values[0], nil)
	if err != nil {
		return api.DevicesInfo{}, err
	}

	onlineTime, err := redigo.String(values[1], nil)
	if err != nil {
		return api.DevicesInfo{}, err
	}

	if err := json.Unmarshal(bytes, &info); err != nil {
		return api.DevicesInfo{}, err
	}

	info.OnlineTime, _ = strconv.ParseFloat(onlineTime, 10)
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

	cKey := fmt.Sprintf("%s;%s", cid, cacheID)
	_, err := rd.cli.SAdd(context.Background(), key, cKey).Result()
	return err
}

// del
func (rd redisDB) RemoveRunningList(cid, cacheID string) error {
	key := fmt.Sprintf(redisKeyRunningList, serverName)

	cKey := fmt.Sprintf("%s;%s", cid, cacheID)
	_, err := rd.cli.SRem(context.Background(), key, cKey).Result()
	return err
}

// SMembers
func (rd redisDB) GetTasksWithList() ([]string, error) {
	key := fmt.Sprintf(redisKeyRunningList, serverName)

	return rd.cli.SMembers(context.Background(), key).Result()
}

// // SISMEMBER
// func (rd redisDB) IsCidInRunningList(cid string) (bool, error) {
// 	key := fmt.Sprintf(redisKeyRunningTask, serverName)

// 	return rd.cli.SIsMember(context.Background(), key, cid).Result()
// }
