package cache

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"time"

	"github.com/fatih/structs"
	"github.com/linguohua/titan/api"

	"github.com/go-redis/redis/v8"
	redigo "github.com/gomodule/redigo/redis"
)

const (
	// redisKeyWaitingDataTaskList
	redisKeyWaitingDataTaskList = "Titan:WaitingDataTaskList"
	// redisKeyValidatorList
	redisKeyValidatorList = "Titan:ValidatorList"
	// redisKeyValidateRoundID
	redisKeyValidateRoundID = "Titan:ValidateRoundID"
	// redisKeyVerifyingList
	redisKeyVerifyingList = "Titan:VerifyingList"
	// redisKeyNodeInfo  deviceID
	redisKeyNodeInfo = "Titan:NodeInfo:%s"
	// redisKeyBlockDownloadRecord serial number
	redisKeyBlockDownloadRecord = "Titan:BlockDownloadRecord:%d"
	// redisKeyBlockDownloadSN
	redisKeyBlockDownloadSN       = "Titan:BlockDownloadRecordSN"
	redisKeyCarfileLatestDownload = "Titan:LatestDownload:%s"
	// redisKeyCacheingCarfileList
	redisKeyCacheingCarfileList = "Titan:CacheingCarfileList"
	// redisKeyCarfileCacheingNodeList hash
	redisKeyCarfileCacheingNodeList = "Titan:CarfileCacheingNodeList:%s"
	// redisKeyCacheingNode  deviceID
	redisKeyCacheingNode = "Titan:CacheingNode:%s"
	// redisKeySystemBaseInfo
	redisKeySystemBaseInfo = "Titan:SystemBaseInfo"

	// redisKeyCarfileRecordCacheResult  hash
	redisKeyCarfileRecordCacheResult = "Titan:CarfileRecordCacheResult:%s"
)

const cacheErrorExpiration = 72 // hour

// TypeRedis redis
func TypeRedis() string {
	return "Redis"
}

var (
	url      string
	redisCli *redis.Client
)

// InitRedis init redis pool
func InitRedis(url string) error {
	url = url

	redisCli = redis.NewClient(&redis.Options{
		Addr:      url,
		Dialer:    nil,
		OnConnect: nil,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := redisCli.Ping(ctx).Result()

	return err
}

func CacheTasksStart(hash string, deviceIDs []string, timeout int64) error {
	ctx := context.Background()

	_, err := redisCli.Pipelined(ctx, func(pipeliner redis.Pipeliner) error {
		pipeliner.SAdd(context.Background(), redisKeyCacheingCarfileList, hash)
		pipeliner.SAdd(context.Background(), redisKeyCarfileCacheingNodeList, deviceIDs)

		for _, deviceID := range deviceIDs {
			nodeKey := fmt.Sprintf(redisKeyCacheingNode, deviceID)
			// Expire
			pipeliner.Set(context.Background(), nodeKey, hash, time.Second*time.Duration(timeout))
		}
		return nil
	})

	return err
}

func CacheTasksEnd(hash string, deviceIDs []string) (bool, error) {
	_, err := redisCli.SRem(context.Background(), redisKeyCarfileCacheingNodeList, deviceIDs).Result()
	if err != nil {
		return false, err
	}

	exist, err := redisCli.Exists(context.Background(), redisKeyCarfileCacheingNodeList).Result()
	if err != nil {
		return false, err
	}

	cachesDone := exist == 0

	if cachesDone {
		_, err = redisCli.SRem(context.Background(), redisKeyCacheingCarfileList, hash).Result()
	}

	return cachesDone, err
}

func UpdateNodeCacheingExpireTime(hash, deviceID string, timeout int64) error {
	nodeKey := fmt.Sprintf(redisKeyCacheingNode, deviceID)
	// Expire
	_, err := redisCli.Set(context.Background(), nodeKey, hash, time.Second*time.Duration(timeout)).Result()
	return err
}

func GetCacheingCarfiles() ([]string, error) {
	return redisCli.SMembers(context.Background(), redisKeyCacheingCarfileList).Result()
}

func GetNodeCacheTimeoutTime(deviceID string) (int, error) {
	key := fmt.Sprintf(redisKeyCacheingNode, deviceID)

	expiration, err := redisCli.TTL(context.Background(), key).Result()
	if err != nil {
		return 0, err
	}

	return int(expiration.Seconds()), nil
}

// carfile record result
func SetCarfileRecordCacheResult(hash string, info *api.CarfileRecordCacheResult) error {
	timeout := 7 * 24
	key := fmt.Sprintf(redisKeyCarfileRecordCacheResult, hash)

	bytes, err := json.Marshal(info)
	if err != nil {
		return err
	}

	// Expire
	_, err = redisCli.Set(context.Background(), key, bytes, time.Hour*time.Duration(timeout)).Result()
	return err
}

func GetCarfileRecordCacheResult(hash string) (*api.CarfileRecordCacheResult, error) {
	key := fmt.Sprintf(redisKeyCarfileRecordCacheResult, hash)

	value, err := redisCli.Get(context.Background(), key).Result()

	var info api.CarfileRecordCacheResult
	bytes, err := redigo.Bytes(value, nil)
	if err != nil {
		return nil, err
	}

	if err := json.Unmarshal(bytes, &info); err != nil {
		return nil, err
	}

	return &info, nil
}

// waiting data list
func PushCarfileToWaitList(info *api.CacheCarfileInfo) error {
	bytes, err := json.Marshal(info)
	if err != nil {
		return err
	}

	_, err = redisCli.RPush(context.Background(), redisKeyWaitingDataTaskList, bytes).Result()
	return err
}

func GetWaitCarfile() (*api.CacheCarfileInfo, error) {
	value, err := redisCli.LPop(context.Background(), redisKeyWaitingDataTaskList).Result()
	if err != nil {
		return nil, err
	}

	var info api.CacheCarfileInfo
	bytes, err := redigo.Bytes(value, nil)
	if err != nil {
		return nil, err
	}

	if err := json.Unmarshal(bytes, &info); err != nil {
		return nil, err
	}

	return &info, nil
}

// validate round id ++1
func IncrValidateRoundID() (int64, error) {
	return redisCli.IncrBy(context.Background(), redisKeyValidateRoundID, 1).Result()
}

// verifying node list
func SetNodesToVerifyingList(deviceIDs []string) error {
	_, err := redisCli.SAdd(context.Background(), redisKeyVerifyingList, deviceIDs).Result()
	return err
}

func GetNodesWithVerifyingList() ([]string, error) {
	return redisCli.SMembers(context.Background(), redisKeyVerifyingList).Result()
}

func CountVerifyingNode() (int64, error) {
	return redisCli.SCard(context.Background(), redisKeyVerifyingList).Result()
}

func RemoveValidatedWithList(deviceID string) error {
	_, err := redisCli.SRem(context.Background(), redisKeyVerifyingList, deviceID).Result()
	return err
}

func RemoveVerifyingList() error {
	_, err := redisCli.Del(context.Background(), redisKeyVerifyingList).Result()
	return err
}

// validator list
func SetValidatorsToList(deviceIDs []string, expiration time.Duration) error {
	// _, err := redisCli.SAdd(context.Background(), key, deviceIDs).Result()

	ctx := context.Background()
	_, err := redisCli.Pipelined(ctx, func(pipeliner redis.Pipeliner) error {
		pipeliner.Del(context.Background(), redisKeyValidatorList)

		if len(deviceIDs) > 0 {
			pipeliner.SAdd(context.Background(), redisKeyValidatorList, deviceIDs)
			// Expire
			pipeliner.PExpire(context.Background(), redisKeyValidatorList, expiration)
		}

		return nil
	})

	return err
}

func GetValidatorsWithList() ([]string, error) {
	return redisCli.SMembers(context.Background(), redisKeyValidatorList).Result()
}

func GetValidatorsAndExpirationTime() ([]string, time.Duration, error) {
	expiration, err := redisCli.TTL(context.Background(), redisKeyValidatorList).Result()
	if err != nil {
		return nil, expiration, err
	}

	list, err := redisCli.SMembers(context.Background(), redisKeyValidatorList).Result()
	if err != nil {
		return nil, expiration, err
	}

	return list, expiration, nil
}

// device info
func IncrNodeOnlineTime(deviceID string, onlineTime int64) (float64, error) {
	key := fmt.Sprintf(redisKeyNodeInfo, deviceID)

	mTime := float64(onlineTime) / 60 // second to minute

	return redisCli.HIncrByFloat(context.Background(), key, "OnlineTime", mTime).Result()
}

func SetDeviceInfo(info *api.DevicesInfo) error {
	key := fmt.Sprintf(redisKeyNodeInfo, info.DeviceId)

	ctx := context.Background()
	exist, err := redisCli.Exists(ctx, key).Result()
	if err != nil {
		return err
	}

	if exist == 1 {
		// update some value
		return updateDeviceInfos(info)
	}

	_, err = redisCli.Pipelined(ctx, func(pipeline redis.Pipeliner) error {
		for field, value := range toMap(info) {
			pipeline.HSet(ctx, key, field, value)
		}
		return nil
	})
	if err != nil {
		return err
	}

	return nil
}

func updateDeviceInfos(info *api.DevicesInfo) error {
	key := fmt.Sprintf(redisKeyNodeInfo, info.DeviceId)

	m := make(map[string]interface{})
	m["DiskSpace"] = info.DiskSpace
	m["DiskUsage"] = info.DiskUsage
	m["ExternalIp"] = info.ExternalIp
	m["InternalIp"] = info.InternalIp
	m["CpuUsage"] = info.CpuUsage
	m["SystemVersion"] = info.SystemVersion
	m["Longitude"] = info.Longitude
	m["Latitude"] = info.Latitude
	m["NatType"] = info.NatType

	_, err := redisCli.HMSet(context.Background(), key, m).Result()
	return err
}

func GetDeviceInfo(deviceID string) (*api.DevicesInfo, error) {
	key := fmt.Sprintf(redisKeyNodeInfo, deviceID)

	var info api.DevicesInfo
	err := redisCli.HGetAll(context.Background(), key).Scan(&info)
	if err != nil {
		return nil, err
	}

	return &info, nil
}

func IncrByDeviceInfo(deviceID, field string, value int64) error {
	key := fmt.Sprintf(redisKeyNodeInfo, deviceID)

	_, err := redisCli.HIncrBy(context.Background(), key, field, value).Result()
	if err != nil {
		return err
	}

	return err
}

// download info
func SetDownloadBlockRecord(record *DownloadBlockRecord) error {
	ctx := context.Background()
	key := fmt.Sprintf(redisKeyBlockDownloadRecord, record.SN)
	_, err := redisCli.HMSet(ctx, key, structs.Map(record)).Result()
	if err != nil {
		return err
	}
	_, err = redisCli.Expire(ctx, key, time.Duration(record.Timeout)*time.Second).Result()
	if err != nil {
		return err
	}

	return nil
}

func GetDownloadBlockRecord(sn int64) (*DownloadBlockRecord, error) {
	key := fmt.Sprintf(redisKeyBlockDownloadRecord, sn)

	var record DownloadBlockRecord

	err := redisCli.HGetAll(context.Background(), key).Scan(&record)
	if err != nil {
		return nil, err
	}

	return &record, nil
}

func RemoveDownloadBlockRecord(sn int64) error {
	key := fmt.Sprintf(redisKeyBlockDownloadRecord, sn)
	_, err := redisCli.Del(context.Background(), key).Result()
	return err
}

func IncrBlockDownloadSN() (int64, error) {
	// node cache tag ++1
	n, err := redisCli.IncrBy(context.Background(), redisKeyBlockDownloadSN, 1).Result()
	if n >= math.MaxInt64 {
		redisCli.Set(context.Background(), redisKeyBlockDownloadSN, 0, 0).Result()
	}

	return n, err
}

// latest data of download
func AddLatestDownloadCarfile(carfileCID string, userIP string) error {
	maxCount := 5
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	key := fmt.Sprintf(redisKeyCarfileLatestDownload, userIP)

	err := redisCli.ZAdd(ctx, key, &redis.Z{Score: float64(time.Now().Unix()), Member: carfileCID}).Err()
	if err != nil {
		return err
	}

	count, err := redisCli.ZCard(ctx, key).Result()
	if err != nil {
		return err
	}

	if count > int64(maxCount) {
		err = redisCli.ZRemRangeByRank(ctx, key, 0, count-int64(maxCount)-1).Err()
		if err != nil {
			return err
		}
	}

	return redisCli.Expire(ctx, key, 24*time.Hour).Err()
}

func GetLatestDownloadCarfiles(userIP string) ([]string, error) {
	key := fmt.Sprintf(redisKeyCarfileLatestDownload, userIP)

	members, err := redisCli.ZRevRange(context.Background(), key, 0, -1).Result()
	if err != nil {
		return []string{}, err
	}
	return members, nil
}

func NodeDownloadCount(deviceID string, blockDownnloadInfo *api.BlockDownloadInfo) error {
	nodeKey := fmt.Sprintf(redisKeyNodeInfo, deviceID)

	ctx := context.Background()
	_, err := redisCli.Pipelined(ctx, func(pipeliner redis.Pipeliner) error {
		pipeliner.HIncrBy(context.Background(), nodeKey, DownloadCountField, 1)
		pipeliner.HIncrBy(context.Background(), nodeKey, totalUploadField, int64(blockDownnloadInfo.BlockSize))

		// count carfile download
		if blockDownnloadInfo.BlockCID == blockDownnloadInfo.CarfileCID {
			pipeliner.HIncrBy(context.Background(), redisKeySystemBaseInfo, DownloadCountField, 1)
		}

		return nil
	})

	return err
}

// IsNilErr Is NilErr
func IsNilErr(err error) bool {
	return errors.Is(err, redis.Nil)
}

func toMap(info *api.DevicesInfo) map[string]interface{} {
	out := make(map[string]interface{})
	t := reflect.TypeOf(*info)
	v := reflect.ValueOf(*info)
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

// system base info
func UpdateSystemBaseInfo(field string, value interface{}) error {
	_, err := redisCli.HSet(context.Background(), redisKeySystemBaseInfo, field, value).Result()
	if err != nil {
		return err
	}

	return err
}

func IncrBySystemBaseInfo(field string, value int64) error {
	_, err := redisCli.HIncrBy(context.Background(), redisKeySystemBaseInfo, field, value).Result()
	if err != nil {
		return err
	}

	return err
}

func GetSystemBaseInfo() (*api.SystemBaseInfo, error) {
	var info api.SystemBaseInfo
	err := redisCli.HGetAll(context.Background(), redisKeySystemBaseInfo).Scan(&info)
	if err != nil {
		return nil, err
	}

	return &info, nil
}

func UpdateNodeCacheInfo(deviceID string, nodeInfo *NodeCacheInfo) error {
	if nodeInfo == nil {
		return nil
	}

	ctx := context.Background()
	_, err := redisCli.Pipelined(ctx, func(pipeliner redis.Pipeliner) error {
		nKey := fmt.Sprintf(redisKeyNodeInfo, deviceID)
		pipeliner.HSet(context.Background(), nKey, diskUsageField, nodeInfo.DiskUsage)
		pipeliner.HSet(context.Background(), nKey, blockCountField, nodeInfo.BlockCount)

		return nil
	})

	return err
}
