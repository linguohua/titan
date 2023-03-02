package cache

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/go-redis/redis/v8"
	redigo "github.com/gomodule/redigo/redis"
	"github.com/linguohua/titan/api"
	"time"
)

const (
	redisKeyCarfileLatestDownload = "Titan:LatestDownload:%s"
	// redisKeyCachingCarfileList
	redisKeyCachingCarfileList = "Titan:CachingCarfileList"
	// redisKeyCarfileCachingNodeList hash
	redisKeyCarfileCachingNodeList = "Titan:CarfileCachingNodeList:%s"
	// redisKeyCachingNode  deviceID
	redisKeyCachingNode = "Titan:CachingNode:%s"
	// redisKeyCarfileRecordCacheResult  hash
	redisKeyCarfileRecordCacheResult = "Titan:CarfileRecordCacheResult:%s"
)

type CarfileCache struct {
	client *redis.Client
}

// NewCarfileCache return new carfile cache instance
func NewCarfileCache(c *redis.Client) *CarfileCache {
	return &CarfileCache{client: c}
}

// ReplicaTasksStart ...
func (c *CarfileCache) ReplicaTasksStart(hash string, deviceIDs []string, timeout int64) error {
	ctx := context.Background()

	_, err := c.client.Pipelined(ctx, func(pipeliner redis.Pipeliner) error {
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

// ReplicaTasksEnd ...
func (c *CarfileCache) ReplicaTasksEnd(hash string, deviceIDs []string) (bool, error) {
	_, err := c.client.SRem(context.Background(), redisKeyCarfileCachingNodeList, deviceIDs).Result()
	if err != nil {
		return false, err
	}

	exist, err := c.client.Exists(context.Background(), redisKeyCarfileCachingNodeList).Result()
	if err != nil {
		return false, err
	}

	cachesDone := exist == 0

	if cachesDone {
		_, err = c.client.SRem(context.Background(), redisKeyCachingCarfileList, hash).Result()
	}

	return cachesDone, err
}

// UpdateNodeCachingExpireTime ...
func (c *CarfileCache) UpdateNodeCachingExpireTime(hash, deviceID string, timeout int64) error {
	nodeKey := fmt.Sprintf(redisKeyCachingNode, deviceID)
	// Expire
	_, err := c.client.Set(context.Background(), nodeKey, hash, time.Second*time.Duration(timeout)).Result()
	return err
}

// GetCachingCarfiles ...
func (c *CarfileCache) GetCachingCarfiles() ([]string, error) {
	return c.client.SMembers(context.Background(), redisKeyCachingCarfileList).Result()
}

// GetNodeCacheTimeoutTime ...
func (c *CarfileCache) GetNodeCacheTimeoutTime(deviceID string) (int, error) {
	key := fmt.Sprintf(redisKeyCachingNode, deviceID)

	expiration, err := c.client.TTL(context.Background(), key).Result()
	if err != nil {
		return 0, err
	}

	return int(expiration.Seconds()), nil
}

// SetCarfileRecordCacheResult carfile record result
func (c *CarfileCache) SetCarfileRecordCacheResult(hash string, info *api.CarfileRecordCacheResult) error {
	timeout := 7 * 24
	key := fmt.Sprintf(redisKeyCarfileRecordCacheResult, hash)

	bytes, err := json.Marshal(info)
	if err != nil {
		return err
	}

	// Expire
	_, err = c.client.Set(context.Background(), key, bytes, time.Hour*time.Duration(timeout)).Result()
	return err
}

// GetCarfileRecordCacheResult ...
func (c *CarfileCache) GetCarfileRecordCacheResult(hash string) (*api.CarfileRecordCacheResult, error) {
	key := fmt.Sprintf(redisKeyCarfileRecordCacheResult, hash)

	value, err := c.client.Get(context.Background(), key).Result()

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

// PushCarfileToWaitList waiting data list
func (c *CarfileCache) PushCarfileToWaitList(info *api.CacheCarfileInfo) error {
	bytes, err := json.Marshal(info)
	if err != nil {
		return err
	}

	_, err = c.client.RPush(context.Background(), redisKeyWaitingDataTaskList, bytes).Result()
	return err
}

// GetWaitCarfile ...
func (c *CarfileCache) GetWaitCarfile() (*api.CacheCarfileInfo, error) {
	value, err := c.client.LPop(context.Background(), redisKeyWaitingDataTaskList).Result()
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

// AddLatestDownloadCarfile latest data of download
func (c *CarfileCache) AddLatestDownloadCarfile(carfileCID string, userIP string) error {
	maxCount := 5
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	key := fmt.Sprintf(redisKeyCarfileLatestDownload, userIP)

	err := c.client.ZAdd(ctx, key, &redis.Z{Score: float64(time.Now().Unix()), Member: carfileCID}).Err()
	if err != nil {
		return err
	}

	count, err := c.client.ZCard(ctx, key).Result()
	if err != nil {
		return err
	}

	if count > int64(maxCount) {
		err = c.client.ZRemRangeByRank(ctx, key, 0, count-int64(maxCount)-1).Err()
		if err != nil {
			return err
		}
	}

	return c.client.Expire(ctx, key, 24*time.Hour).Err()
}

// GetLatestDownloadCarfiles ...
func (c *CarfileCache) GetLatestDownloadCarfiles(userIP string) ([]string, error) {
	key := fmt.Sprintf(redisKeyCarfileLatestDownload, userIP)

	members, err := c.client.ZRevRange(context.Background(), key, 0, -1).Result()
	if err != nil {
		return []string{}, err
	}
	return members, nil
}
