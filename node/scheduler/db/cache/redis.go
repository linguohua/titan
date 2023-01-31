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

	// redisKeyBaseInfo  server name
	redisKeyBaseInfo = "Titan:BaseInfo:%s"
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

func (rd redisDB) CacheTasksStart(hash string, deviceIDs []string, timeout int64) error {
	cacheingCarfileList := fmt.Sprintf(redisKeyCacheingCarfileList, serverName)
	carfileNodeList := fmt.Sprintf(redisKeyCarfileCacheingNodeList, serverName, hash)

	ctx := context.Background()

	_, err := rd.cli.Pipelined(ctx, func(pipeliner redis.Pipeliner) error {
		pipeliner.SAdd(context.Background(), cacheingCarfileList, hash)
		pipeliner.SAdd(context.Background(), carfileNodeList, deviceIDs)

		for _, deviceID := range deviceIDs {
			nodeKey := fmt.Sprintf(redisKeyCacheingNode, serverName, deviceID)
			// Expire
			pipeliner.Set(context.Background(), nodeKey, hash, time.Second*time.Duration(timeout))
		}
		return nil
	})

	return err
}

func (rd redisDB) CacheTasksEnd(hash string, deviceIDs []string) (bool, error) {
	cacheingCarfileList := fmt.Sprintf(redisKeyCacheingCarfileList, serverName)
	carfileNodeList := fmt.Sprintf(redisKeyCarfileCacheingNodeList, serverName, hash)

	_, err := rd.cli.SRem(context.Background(), carfileNodeList, deviceIDs).Result()
	if err != nil {
		return false, err
	}

	exist, err := rd.cli.Exists(context.Background(), carfileNodeList).Result()
	if err != nil {
		return false, err
	}

	cachesDone := exist == 0

	ctx := context.Background()
	_, err = rd.cli.Pipelined(ctx, func(pipeliner redis.Pipeliner) error {
		if cachesDone {
			pipeliner.SRem(context.Background(), cacheingCarfileList, hash)
		}

		for _, deviceID := range deviceIDs {
			nodeKey := fmt.Sprintf(redisKeyCacheingNode, serverName, deviceID)
			// Expire
			pipeliner.Del(context.Background(), nodeKey)
		}

		// if nodeInfo != nil {
		// 	nKey := fmt.Sprintf(redisKeyNodeInfo, nodeInfo.DeviceID)
		// 	pipeliner.HMSet(context.Background(), nKey, diskUsageField, nodeInfo.DiskUsage, blockCountField, nodeInfo.BlockCount, totalDownloadField, nodeInfo.TotalDownload)
		// 	if nodeInfo.IsSuccess {
		// 		baseInfoKey := fmt.Sprintf(redisKeyBaseInfo, serverName)
		// 		pipeliner.HIncrBy(context.Background(), baseInfoKey, CarFileCountField, 1)
		// 	}
		// }

		return nil
	})

	return cachesDone, err
}

func (rd redisDB) UpdateNodeCacheingExpireTime(hash, deviceID string, timeout int64) error {
	nodeKey := fmt.Sprintf(redisKeyCacheingNode, serverName, deviceID)
	// Expire
	_, err := rd.cli.Set(context.Background(), nodeKey, hash, time.Second*time.Duration(timeout)).Result()
	return err
}

func (rd redisDB) GetCacheingCarfiles() ([]string, error) {
	cacheingCarfileList := fmt.Sprintf(redisKeyCacheingCarfileList, serverName)
	return rd.cli.SMembers(context.Background(), cacheingCarfileList).Result()
}

func (rd redisDB) IsNodeCaching(deviceID string) (bool, error) {
	nodeKey := fmt.Sprintf(redisKeyCacheingNode, serverName, deviceID)
	exist, err := rd.cli.Exists(context.Background(), nodeKey).Result()
	if err != nil {
		return false, err
	}

	return exist == 1, nil
}

// waiting data list
func (rd redisDB) PushCarfileToWaitList(info *api.CacheCarfileInfo) error {
	key := fmt.Sprintf(redisKeyWaitingDataTaskList, serverName)

	bytes, err := json.Marshal(info)
	if err != nil {
		return err
	}

	_, err = rd.cli.RPush(context.Background(), key, bytes).Result()
	return err
}

func (rd redisDB) GetWaitCarfile() (*api.CacheCarfileInfo, error) {
	key := fmt.Sprintf(redisKeyWaitingDataTaskList, serverName)

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
	key := fmt.Sprintf(redisKeyValidateRoundID, serverName)

	return rd.cli.IncrBy(context.Background(), key, 1).Result()
}

// verifying node list
func (rd redisDB) SetNodesToVerifyingList(deviceIDs []string) error {
	key := fmt.Sprintf(redisKeyVerifyingList, serverName)

	_, err := rd.cli.SAdd(context.Background(), key, deviceIDs).Result()
	return err
}

func (rd redisDB) GetNodesWithVerifyingList() ([]string, error) {
	key := fmt.Sprintf(redisKeyVerifyingList, serverName)

	return rd.cli.SMembers(context.Background(), key).Result()
}

// func (rd redisDB) CountVerifyingNode(ctx context.Context) (int64, error) {
// 	key := fmt.Sprintf(redisKeyVerifyingList, serverName)
// 	return rd.cli.SCard(ctx, key).Result()
// }

func (rd redisDB) RemoveValidatedWithList(deviceID string) (int64, error) {
	key := fmt.Sprintf(redisKeyVerifyingList, serverName)

	_, err := rd.cli.SRem(context.Background(), key, deviceID).Result()
	if err != nil {
		return 0, err
	}

	return rd.cli.SCard(context.Background(), key).Result()
}

func (rd redisDB) RemoveVerifyingList() error {
	key := fmt.Sprintf(redisKeyVerifyingList, serverName)

	_, err := rd.cli.Del(context.Background(), key).Result()
	return err
}

// validator list
func (rd redisDB) SetValidatorsToList(deviceIDs []string, expiration time.Duration) error {
	key := fmt.Sprintf(redisKeyValidatorList, serverName)

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
	key := fmt.Sprintf(redisKeyValidatorList, serverName)

	return rd.cli.SMembers(context.Background(), key).Result()
}

func (rd redisDB) GetValidatorsAndExpirationTime() ([]string, time.Duration, error) {
	key := fmt.Sprintf(redisKeyValidatorList, serverName)

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

	// _, err = rd.cli.HMSet(ctx, key, structs.Map(info)).Result()
	// if err != nil {
	// 	return err
	// }

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
	m["Longitude"] = info.Longitude
	m["Latitude"] = info.Latitude

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
			key := fmt.Sprintf(redisKeyBaseInfo, serverName)
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
func (rd redisDB) UpdateBaseInfo(field string, value interface{}) error {
	key := fmt.Sprintf(redisKeyBaseInfo, serverName)

	_, err := rd.cli.HSet(context.Background(), key, field, value).Result()
	if err != nil {
		return err
	}

	return err
}

func (rd redisDB) IncrByBaseInfo(field string, value int64) error {
	key := fmt.Sprintf(redisKeyBaseInfo, serverName)

	_, err := rd.cli.HIncrBy(context.Background(), key, field, value).Result()
	if err != nil {
		return err
	}

	return err
}

func (rd redisDB) GetBaseInfo() (*api.BaseInfo, error) {
	key := fmt.Sprintf(redisKeyBaseInfo, serverName)

	var info api.BaseInfo
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
