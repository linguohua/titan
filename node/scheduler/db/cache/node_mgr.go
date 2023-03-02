package cache

import (
	"context"
	"errors"
	"fmt"
	"github.com/go-redis/redis/v8"
	"github.com/linguohua/titan/api"
	"math"
	"reflect"
	"time"
)

const (
	redisKeyNodeInfo = "Titan:NodeInfo:%s"
	// redisKeyBlockDownloadSN
	redisKeyBlockDownloadSN = "Titan:BlockDownloadRecordSN"
	// redisKeySystemBaseInfo
	redisKeySystemBaseInfo = "Titan:SystemBaseInfo"
	// redisKeyWaitingDataTaskList
	redisKeyWaitingDataTaskList = "Titan:WaitingDataTaskList"
	// redisKeyValidatorList
	redisKeyValidatorList = "Titan:ValidatorList"
	// redisKeyValidateRoundID
	redisKeyValidateRoundID = "Titan:ValidateRoundID"
	// redisKeyVerifyingList
	redisKeyVerifyingList = "Titan:VerifyingList"
)

type NodeMgrCache struct {
	client *redis.Client
}

// NewNodeManagerCache return new node manager cache instance
func NewNodeManagerCache(c *redis.Client) *NodeMgrCache {
	return &NodeMgrCache{client: c}
}

// IncrNodeOnlineTime ...
func (c *NodeMgrCache) IncrNodeOnlineTime(deviceID string, onlineTime int64) (float64, error) {
	key := fmt.Sprintf(redisKeyNodeInfo, deviceID)

	mTime := float64(onlineTime) / 60 // second to minute

	return c.client.HIncrByFloat(context.Background(), key, "OnlineTime", mTime).Result()
}

// SetDeviceInfo ...
func (c *NodeMgrCache) SetDeviceInfo(info *api.DeviceInfo) error {
	key := fmt.Sprintf(redisKeyNodeInfo, info.DeviceID)

	ctx := context.Background()
	exist, err := c.client.Exists(ctx, key).Result()
	if err != nil {
		return err
	}

	if exist == 1 {
		// update some value
		return c.updateDeviceInfos(info)
	}

	_, err = c.client.Pipelined(ctx, func(pipeline redis.Pipeliner) error {
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

func (c *NodeMgrCache) updateDeviceInfos(info *api.DeviceInfo) error {
	key := fmt.Sprintf(redisKeyNodeInfo, info.DeviceID)

	m := make(map[string]interface{})
	m["DiskSpace"] = info.DiskSpace
	m["DiskUsage"] = info.DiskUsage
	m["ExternalIp"] = info.ExternalIP
	m["InternalIp"] = info.InternalIP
	m["CpuUsage"] = info.CPUUsage
	m["SystemVersion"] = info.SystemVersion
	m["Longitude"] = info.Longitude
	m["Latitude"] = info.Latitude
	m["NatType"] = info.NatType

	_, err := c.client.HMSet(context.Background(), key, m).Result()
	return err
}

// GetDeviceInfo ...
func (c *NodeMgrCache) GetDeviceInfo(deviceID string) (*api.DeviceInfo, error) {
	key := fmt.Sprintf(redisKeyNodeInfo, deviceID)

	var info api.DeviceInfo
	err := c.client.HGetAll(context.Background(), key).Scan(&info)
	if err != nil {
		return nil, err
	}

	return &info, nil
}

// IncrByDeviceInfo ...
func (c *NodeMgrCache) IncrByDeviceInfo(deviceID, field string, value int64) error {
	key := fmt.Sprintf(redisKeyNodeInfo, deviceID)

	_, err := c.client.HIncrBy(context.Background(), key, field, value).Result()
	if err != nil {
		return err
	}

	return err
}

// IncrBlockDownloadSN ...
func (c *NodeMgrCache) IncrBlockDownloadSN() (int64, error) {
	// node cache tag ++1
	n, err := c.client.IncrBy(context.Background(), redisKeyBlockDownloadSN, 1).Result()
	if n >= math.MaxInt64 {
		c.client.Set(context.Background(), redisKeyBlockDownloadSN, 0, 0).Result()
	}

	return n, err
}

// NodeDownloadCount ...
func (c *NodeMgrCache) NodeDownloadCount(deviceID string, blockDownnloadInfo *api.BlockDownloadInfo) error {
	nodeKey := fmt.Sprintf(redisKeyNodeInfo, deviceID)

	ctx := context.Background()
	_, err := c.client.Pipelined(ctx, func(pipeliner redis.Pipeliner) error {
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

func toMap(info *api.DeviceInfo) map[string]interface{} {
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

// UpdateSystemBaseInfo system base info
func (c *NodeMgrCache) UpdateSystemBaseInfo(field string, value interface{}) error {
	_, err := c.client.HSet(context.Background(), redisKeySystemBaseInfo, field, value).Result()
	if err != nil {
		return err
	}

	return err
}

// IncrBySystemBaseInfo ...
func (c *NodeMgrCache) IncrBySystemBaseInfo(field string, value int64) error {
	_, err := c.client.HIncrBy(context.Background(), redisKeySystemBaseInfo, field, value).Result()
	if err != nil {
		return err
	}

	return err
}

// GetSystemBaseInfo ...
func (c *NodeMgrCache) GetSystemBaseInfo() (*api.SystemBaseInfo, error) {
	var info api.SystemBaseInfo
	err := c.client.HGetAll(context.Background(), redisKeySystemBaseInfo).Scan(&info)
	if err != nil {
		return nil, err
	}

	return &info, nil
}

// UpdateNodeCacheInfo ...
func (c *NodeMgrCache) UpdateNodeCacheInfo(deviceID string, nodeInfo *NodeCacheInfo) error {
	if nodeInfo == nil {
		return nil
	}

	ctx := context.Background()
	_, err := c.client.Pipelined(ctx, func(pipeliner redis.Pipeliner) error {
		nKey := fmt.Sprintf(redisKeyNodeInfo, deviceID)
		pipeliner.HSet(context.Background(), nKey, diskUsageField, nodeInfo.DiskUsage)
		pipeliner.HSet(context.Background(), nKey, blockCountField, nodeInfo.BlockCount)

		return nil
	})

	return err
}

// IncrValidateRoundID validator round id ++1
func (c *NodeMgrCache) IncrValidateRoundID() (int64, error) {
	return c.client.IncrBy(context.Background(), redisKeyValidateRoundID, 1).Result()
}

// SetNodesToVerifyingList verifying node list
func (c *NodeMgrCache) SetNodesToVerifyingList(deviceIDs []string) error {
	_, err := c.client.SAdd(context.Background(), redisKeyVerifyingList, deviceIDs).Result()
	return err
}

// GetNodesWithVerifyingList ...
func (c *NodeMgrCache) GetNodesWithVerifyingList() ([]string, error) {
	return c.client.SMembers(context.Background(), redisKeyVerifyingList).Result()
}

// CountVerifyingNode ...
func (c *NodeMgrCache) CountVerifyingNode() (int64, error) {
	return c.client.SCard(context.Background(), redisKeyVerifyingList).Result()
}

// RemoveValidatedWithList ...
func (c *NodeMgrCache) RemoveValidatedWithList(deviceID string) error {
	_, err := c.client.SRem(context.Background(), redisKeyVerifyingList, deviceID).Result()
	return err
}

// RemoveVerifyingList ...
func (c *NodeMgrCache) RemoveVerifyingList() error {
	_, err := c.client.Del(context.Background(), redisKeyVerifyingList).Result()
	return err
}

// SetValidatorsToList validator list
func (c *NodeMgrCache) SetValidatorsToList(deviceIDs []string, expiration time.Duration) error {
	ctx := context.Background()
	_, err := c.client.Pipelined(ctx, func(pipeliner redis.Pipeliner) error {
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

// GetValidatorsWithList ...
func (c *NodeMgrCache) GetValidatorsWithList() ([]string, error) {
	return c.client.SMembers(context.Background(), redisKeyValidatorList).Result()
}

// GetValidatorsAndExpirationTime ...
func (c *NodeMgrCache) GetValidatorsAndExpirationTime() ([]string, time.Duration, error) {
	expiration, err := c.client.TTL(context.Background(), redisKeyValidatorList).Result()
	if err != nil {
		return nil, expiration, err
	}

	list, err := c.client.SMembers(context.Background(), redisKeyValidatorList).Result()
	if err != nil {
		return nil, expiration, err
	}

	return list, expiration, nil
}
