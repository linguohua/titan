package cache

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"time"

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
	// redisKeyBlockDownloadSN
	redisKeyBlockDownloadSN       = "Titan:BlockDownloadRecordSN"
	redisKeyCarfileLatestDownload = "Titan:LatestDownload:%s"
	// redisKeyCachingCarfileList
	redisKeyCachingCarfileList = "Titan:CachingCarfileList"
	// redisKeyCarfileCachingNodeList hash
	redisKeyCarfileCachingNodeList = "Titan:CarfileCachingNodeList:%s"
	// redisKeyCachingNode  deviceID
	redisKeyCachingNode = "Titan:CachingNode:%s"
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

func ReplicaTasksStart(hash string, deviceIDs []string, timeout int64) error {
	ctx := context.Background()

	_, err := redisCli.Pipelined(ctx, func(pipeliner redis.Pipeliner) error {
		pipeliner.SAdd(context.Background(), redisKeyCachingCarfileList, hash)
		pipeliner.SAdd(context.Background(), redisKeyCarfileCachingNodeList, deviceIDs)

		for _, deviceID := range deviceIDs {
			nodeKey := fmt.Sprintf(redisKeyCachingNode, deviceID)
			// Expire
			pipeliner.Set(context.Background(), nodeKey, hash, time.Second*time.Duration(timeout))
		}
		return nil
	})

	return err
}

func ReplicaTasksEnd(hash string, deviceIDs []string) (bool, error) {
	_, err := redisCli.SRem(context.Background(), redisKeyCarfileCachingNodeList, deviceIDs).Result()
	if err != nil {
		return false, err
	}

	exist, err := redisCli.Exists(context.Background(), redisKeyCarfileCachingNodeList).Result()
	if err != nil {
		return false, err
	}

	cachesDone := exist == 0

	if cachesDone {
		_, err = redisCli.SRem(context.Background(), redisKeyCachingCarfileList, hash).Result()
	}

	return cachesDone, err
}

func UpdateNodeCachingExpireTime(hash, deviceID string, timeout int64) error {
	nodeKey := fmt.Sprintf(redisKeyCachingNode, deviceID)
	// Expire
	_, err := redisCli.Set(context.Background(), nodeKey, hash, time.Second*time.Duration(timeout)).Result()
	return err
}

func GetCachingCarfiles() ([]string, error) {
	return redisCli.SMembers(context.Background(), redisKeyCachingCarfileList).Result()
}

func GetNodeCacheTimeoutTime(deviceID string) (int, error) {
	key := fmt.Sprintf(redisKeyCachingNode, deviceID)

	expiration, err := redisCli.TTL(context.Background(), key).Result()
	if err != nil {
		return 0, err
	}

	return int(expiration.Seconds()), nil
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

// func IncrByDeviceInfo(deviceID, field string, value int64) error {
// 	key := fmt.Sprintf(redisKeyNodeInfo, deviceID)

// 	_, err := redisCli.HIncrBy(context.Background(), key, field, value).Result()
// 	if err != nil {
// 		return err
// 	}

// 	return err
// }

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

// func NodeDownloadCount(deviceID string, blockDownnloadInfo *api.BlockDownloadInfo) error {
// 	nodeKey := fmt.Sprintf(redisKeyNodeInfo, deviceID)

// 	ctx := context.Background()
// 	_, err := redisCli.Pipelined(ctx, func(pipeliner redis.Pipeliner) error {
// 		pipeliner.HIncrBy(context.Background(), nodeKey, DownloadCountField, 1)
// 		pipeliner.HIncrBy(context.Background(), nodeKey, totalUploadField, int64(blockDownnloadInfo.BlockSize))

// 		// count carfile download
// 		if blockDownnloadInfo.BlockCID == blockDownnloadInfo.CarfileCID {
// 			pipeliner.HIncrBy(context.Background(), redisKeySystemBaseInfo, DownloadCountField, 1)
// 		}

// 		return nil
// 	})

// 	return err
// }

// IsNilErr Is NilErr
func IsNilErr(err error) bool {
	return errors.Is(err, redis.Nil)
}

// func UpdateNodeCacheInfo(deviceID string, nodeInfo *NodeCacheInfo) error {
// 	if nodeInfo == nil {
// 		return nil
// 	}

// 	ctx := context.Background()
// 	_, err := redisCli.Pipelined(ctx, func(pipeliner redis.Pipeliner) error {
// 		nKey := fmt.Sprintf(redisKeyNodeInfo, deviceID)
// 		pipeliner.HSet(context.Background(), nKey, diskUsageField, nodeInfo.DiskUsage)
// 		pipeliner.HSet(context.Background(), nKey, blockCountField, nodeInfo.BlockCount)

// 		return nil
// 	})

// 	return err
// }
