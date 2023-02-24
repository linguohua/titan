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
	// redisKeyWaitingDataTaskList  server name
	redisKeyWaitingDataTaskList = "Titan:WaitingDataTaskList:%s"
	// redisKeyValidatorList server name
	redisKeyValidatorList = "Titan:ValidatorList:%s"
	// redisKeyValidateRoundID server name
	redisKeyValidateRoundID = "Titan:ValidateRoundID:%s"
	// redisKeyVerifyingList server name
	redisKeyVerifyingList = "Titan:VerifyingList:%s"
	// redisKeyNodeInfo  deviceID
	redisKeyNodeInfo = "Titan:NodeInfo:%s"
	// redisKeyBlockDownloadRecord serial number
	redisKeyBlockDownloadRecord = "Titan:BlockDownloadRecord:%d"
	// redisKeyBlockDownloadSN
	redisKeyBlockDownloadSN       = "Titan:BlockDownloadRecordSN"
	redisKeyCarfileLatestDownload = "Titan:LatestDownload:%s"

	// redisKeyCacheingCarfileList  server name
	redisKeyCacheingCarfileList = "Titan:CacheingCarfileList:%s"
	// redisKeyCarfileCacheingNodeList  server name:hash
	redisKeyCarfileCacheingNodeList = "Titan:CarfileCacheingNodeList:%s:%s"
	// redisKeyCacheingNode  server name:deviceID
	redisKeyCacheingNode = "Titan:CacheingNode:%s:%s"

	// redisKeySystemBaseInfo  server name
	redisKeySystemBaseInfo = "Titan:SystemBaseInfo:%s"

	// redisKeyCarfileRecordCacheResult  hash
	redisKeyCarfileRecordCacheResult = "Titan:CarfileRecordCacheResult:%s"
)

const cacheErrorExpiration = 72 // hour

// TypeRedis redis
func TypeRedis() string {
	return "Redis"
}

type redisDB struct {
	cli *redis.Client
}

// InitRedis init redis pool
func InitRedis(url string) (DB, error) {
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

func (rd redisDB) CacheTasksStart(hash string, deviceIDs []string, timeout int64) error {
	cacheingCarfileList := fmt.Sprintf(redisKeyCacheingCarfileList, serverID)
	carfileNodeList := fmt.Sprintf(redisKeyCarfileCacheingNodeList, serverID, hash)

	ctx := context.Background()

	_, err := rd.cli.Pipelined(ctx, func(pipeliner redis.Pipeliner) error {
		pipeliner.SAdd(context.Background(), cacheingCarfileList, hash)
		pipeliner.SAdd(context.Background(), carfileNodeList, deviceIDs)

		for _, deviceID := range deviceIDs {
			nodeKey := fmt.Sprintf(redisKeyCacheingNode, serverID, deviceID)
			// Expire
			pipeliner.Set(context.Background(), nodeKey, hash, time.Second*time.Duration(timeout))
		}
		return nil
	})

	return err
}

func (rd redisDB) CacheTasksEnd(hash string, deviceIDs []string) (bool, error) {
	cacheingCarfileList := fmt.Sprintf(redisKeyCacheingCarfileList, serverID)
	carfileNodeList := fmt.Sprintf(redisKeyCarfileCacheingNodeList, serverID, hash)

	_, err := rd.cli.SRem(context.Background(), carfileNodeList, deviceIDs).Result()
	if err != nil {
		return false, err
	}

	exist, err := rd.cli.Exists(context.Background(), carfileNodeList).Result()
	if err != nil {
		return false, err
	}

	cachesDone := exist == 0

	if cachesDone {
		_, err = rd.cli.SRem(context.Background(), cacheingCarfileList, hash).Result()
	}

	return cachesDone, err
}

func (rd redisDB) UpdateNodeCacheingExpireTime(hash, deviceID string, timeout int64) error {
	nodeKey := fmt.Sprintf(redisKeyCacheingNode, serverID, deviceID)
	// Expire
	_, err := rd.cli.Set(context.Background(), nodeKey, hash, time.Second*time.Duration(timeout)).Result()
	return err
}

func (rd redisDB) GetCacheingCarfiles() ([]string, error) {
	cacheingCarfileList := fmt.Sprintf(redisKeyCacheingCarfileList, serverID)
	return rd.cli.SMembers(context.Background(), cacheingCarfileList).Result()
}

func (rd redisDB) GetNodeCacheTimeoutTime(deviceID string) (int, error) {
	key := fmt.Sprintf(redisKeyCacheingNode, serverID, deviceID)

	expiration, err := rd.cli.TTL(context.Background(), key).Result()
	if err != nil {
		return 0, err
	}

	return int(expiration.Seconds()), nil
}

// carfile record result
func (rd redisDB) SetCarfileRecordCacheResult(hash string, info *api.CarfileRecordCacheResult) error {
	timeout := 7 * 24
	key := fmt.Sprintf(redisKeyCarfileRecordCacheResult, hash)

	bytes, err := json.Marshal(info)
	if err != nil {
		return err
	}

	// Expire
	_, err = rd.cli.Set(context.Background(), key, bytes, time.Hour*time.Duration(timeout)).Result()
	return err
}

func (rd redisDB) GetCarfileRecordCacheResult(hash string) (*api.CarfileRecordCacheResult, error) {
	key := fmt.Sprintf(redisKeyCarfileRecordCacheResult, hash)

	value, err := rd.cli.Get(context.Background(), key).Result()

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
func (rd redisDB) PushCarfileToWaitList(info *api.CacheCarfileInfo) error {
	key := fmt.Sprintf(redisKeyWaitingDataTaskList, serverID)

	bytes, err := json.Marshal(info)
	if err != nil {
		return err
	}

	_, err = rd.cli.RPush(context.Background(), key, bytes).Result()
	return err
}

func (rd redisDB) GetWaitCarfile() (*api.CacheCarfileInfo, error) {
	key := fmt.Sprintf(redisKeyWaitingDataTaskList, serverID)

	value, err := rd.cli.LPop(context.Background(), key).Result()
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
func (rd redisDB) IncrValidateRoundID() (int64, error) {
	key := fmt.Sprintf(redisKeyValidateRoundID, serverID)

	return rd.cli.IncrBy(context.Background(), key, 1).Result()
}

// verifying node list
func (rd redisDB) SetNodesToVerifyingList(deviceIDs []string) error {
	key := fmt.Sprintf(redisKeyVerifyingList, serverID)

	_, err := rd.cli.SAdd(context.Background(), key, deviceIDs).Result()
	return err
}

func (rd redisDB) GetNodesWithVerifyingList() ([]string, error) {
	key := fmt.Sprintf(redisKeyVerifyingList, serverID)

	return rd.cli.SMembers(context.Background(), key).Result()
}

func (rd redisDB) CountVerifyingNode() (int64, error) {
	key := fmt.Sprintf(redisKeyVerifyingList, serverID)
	return rd.cli.SCard(context.Background(), key).Result()
}

func (rd redisDB) RemoveValidatedWithList(deviceID string) error {
	key := fmt.Sprintf(redisKeyVerifyingList, serverID)

	_, err := rd.cli.SRem(context.Background(), key, deviceID).Result()
	return err
}

func (rd redisDB) RemoveVerifyingList() error {
	key := fmt.Sprintf(redisKeyVerifyingList, serverID)

	_, err := rd.cli.Del(context.Background(), key).Result()
	return err
}

// validator list
func (rd redisDB) SetValidatorsToList(deviceIDs []string, expiration time.Duration) error {
	key := fmt.Sprintf(redisKeyValidatorList, serverID)

	// _, err := rd.cli.SAdd(context.Background(), key, deviceIDs).Result()

	ctx := context.Background()
	_, err := rd.cli.Pipelined(ctx, func(pipeliner redis.Pipeliner) error {
		pipeliner.Del(context.Background(), key)

		if len(deviceIDs) > 0 {
			pipeliner.SAdd(context.Background(), key, deviceIDs)
			// Expire
			pipeliner.PExpire(context.Background(), key, expiration)
		}

		return nil
	})

	return err
}

func (rd redisDB) GetValidatorsWithList() ([]string, error) {
	key := fmt.Sprintf(redisKeyValidatorList, serverID)

	return rd.cli.SMembers(context.Background(), key).Result()
}

func (rd redisDB) GetValidatorsAndExpirationTime() ([]string, time.Duration, error) {
	key := fmt.Sprintf(redisKeyValidatorList, serverID)

	expiration, err := rd.cli.TTL(context.Background(), key).Result()
	if err != nil {
		return nil, expiration, err
	}

	list, err := rd.cli.SMembers(context.Background(), key).Result()
	if err != nil {
		return nil, expiration, err
	}

	return list, expiration, nil
}

// device info
func (rd redisDB) IncrNodeOnlineTime(deviceID string, onlineTime int64) (float64, error) {
	key := fmt.Sprintf(redisKeyNodeInfo, deviceID)

	mTime := float64(onlineTime) / 60 // second to minute

	return rd.cli.HIncrByFloat(context.Background(), key, "OnlineTime", mTime).Result()
}

func (rd redisDB) SetDeviceInfo(info *api.DevicesInfo) error {
	key := fmt.Sprintf(redisKeyNodeInfo, info.DeviceId)

	ctx := context.Background()
	exist, err := rd.cli.Exists(ctx, key).Result()
	if err != nil {
		return err
	}

	if exist == 1 {
		// update some value
		return rd.updateDeviceInfos(info)
	}

	_, err = rd.cli.Pipelined(ctx, func(pipeline redis.Pipeliner) error {
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

func (rd redisDB) updateDeviceInfos(info *api.DevicesInfo) error {
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

	_, err := rd.cli.HMSet(context.Background(), key, m).Result()
	return err
}

func (rd redisDB) GetDeviceInfo(deviceID string) (*api.DevicesInfo, error) {
	key := fmt.Sprintf(redisKeyNodeInfo, deviceID)

	var info api.DevicesInfo
	err := rd.cli.HGetAll(context.Background(), key).Scan(&info)
	if err != nil {
		return nil, err
	}

	return &info, nil
}

func (rd redisDB) IncrByDeviceInfo(deviceID, field string, value int64) error {
	key := fmt.Sprintf(redisKeyNodeInfo, deviceID)

	_, err := rd.cli.HIncrBy(context.Background(), key, field, value).Result()
	if err != nil {
		return err
	}

	return err
}

// download info
func (rd redisDB) SetDownloadBlockRecord(record *DownloadBlockRecord) error {
	ctx := context.Background()
	key := fmt.Sprintf(redisKeyBlockDownloadRecord, record.SN)
	_, err := rd.cli.HMSet(ctx, key, structs.Map(record)).Result()
	if err != nil {
		return err
	}
	_, err = rd.cli.Expire(ctx, key, time.Duration(record.Timeout)*time.Second).Result()
	if err != nil {
		return err
	}

	return nil
}

func (rd redisDB) GetDownloadBlockRecord(sn int64) (*DownloadBlockRecord, error) {
	key := fmt.Sprintf(redisKeyBlockDownloadRecord, sn)

	var record DownloadBlockRecord

	err := rd.cli.HGetAll(context.Background(), key).Scan(&record)
	if err != nil {
		return nil, err
	}

	return &record, nil
}

func (rd redisDB) RemoveDownloadBlockRecord(sn int64) error {
	key := fmt.Sprintf(redisKeyBlockDownloadRecord, sn)
	_, err := rd.cli.Del(context.Background(), key).Result()
	return err
}

func (rd redisDB) IncrBlockDownloadSN() (int64, error) {
	// node cache tag ++1
	n, err := rd.cli.IncrBy(context.Background(), redisKeyBlockDownloadSN, 1).Result()
	if n >= math.MaxInt64 {
		rd.cli.Set(context.Background(), redisKeyBlockDownloadSN, 0, 0).Result()
	}

	return n, err
}

// latest data of download
func (rd redisDB) AddLatestDownloadCarfile(carfileCID string, userIP string) error {
	maxCount := 5
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	key := fmt.Sprintf(redisKeyCarfileLatestDownload, userIP)

	err := rd.cli.ZAdd(ctx, key, &redis.Z{Score: float64(time.Now().Unix()), Member: carfileCID}).Err()
	if err != nil {
		return err
	}

	count, err := rd.cli.ZCard(ctx, key).Result()
	if err != nil {
		return err
	}

	if count > int64(maxCount) {
		err = rd.cli.ZRemRangeByRank(ctx, key, 0, count-int64(maxCount)-1).Err()
		if err != nil {
			return err
		}
	}

	return rd.cli.Expire(ctx, key, 24*time.Hour).Err()
}

func (rd redisDB) GetLatestDownloadCarfiles(userIP string) ([]string, error) {
	key := fmt.Sprintf(redisKeyCarfileLatestDownload, userIP)

	members, err := rd.cli.ZRevRange(context.Background(), key, 0, -1).Result()
	if err != nil {
		return []string{}, err
	}
	return members, nil
}

func (rd redisDB) NodeDownloadCount(deviceID string, blockDownnloadInfo *api.BlockDownloadInfo) error {
	nodeKey := fmt.Sprintf(redisKeyNodeInfo, deviceID)

	ctx := context.Background()
	_, err := rd.cli.Pipelined(ctx, func(pipeliner redis.Pipeliner) error {
		pipeliner.HIncrBy(context.Background(), nodeKey, DownloadCountField, 1)
		pipeliner.HIncrBy(context.Background(), nodeKey, totalUploadField, int64(blockDownnloadInfo.BlockSize))

		// count carfile download
		if blockDownnloadInfo.BlockCID == blockDownnloadInfo.CarfileCID {
			key := fmt.Sprintf(redisKeySystemBaseInfo, serverID)
			pipeliner.HIncrBy(context.Background(), key, DownloadCountField, 1)
		}

		return nil
	})

	return err
}

// IsNilErr Is NilErr
func (rd redisDB) IsNilErr(err error) bool {
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
func (rd redisDB) UpdateSystemBaseInfo(field string, value interface{}) error {
	key := fmt.Sprintf(redisKeySystemBaseInfo, serverID)

	_, err := rd.cli.HSet(context.Background(), key, field, value).Result()
	if err != nil {
		return err
	}

	return err
}

func (rd redisDB) IncrBySystemBaseInfo(field string, value int64) error {
	key := fmt.Sprintf(redisKeySystemBaseInfo, serverID)

	_, err := rd.cli.HIncrBy(context.Background(), key, field, value).Result()
	if err != nil {
		return err
	}

	return err
}

func (rd redisDB) GetSystemBaseInfo() (*api.SystemBaseInfo, error) {
	key := fmt.Sprintf(redisKeySystemBaseInfo, serverID)

	var info api.SystemBaseInfo
	err := rd.cli.HGetAll(context.Background(), key).Scan(&info)
	if err != nil {
		return nil, err
	}

	return &info, nil
}

func (rd redisDB) UpdateNodeCacheInfo(deviceID string, nodeInfo *NodeCacheInfo) error {
	if nodeInfo == nil {
		return nil
	}

	ctx := context.Background()
	_, err := rd.cli.Pipelined(ctx, func(pipeliner redis.Pipeliner) error {
		nKey := fmt.Sprintf(redisKeyNodeInfo, deviceID)
		pipeliner.HSet(context.Background(), nKey, diskUsageField, nodeInfo.DiskUsage)
		pipeliner.HSet(context.Background(), nKey, blockCountField, nodeInfo.BlockCount)

		return nil
	})

	return err
}
