package cache

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"time"

	"github.com/linguohua/titan/api"

	"github.com/fatih/structs"
	"github.com/go-redis/redis/v8"
	redigo "github.com/gomodule/redigo/redis"
)

const (
	// redisKeyWaitingDataTaskList  server name
	redisKeyWaitingDataTaskList = "Titan:WaitingDataTaskList:%s"
	// redisKeyRunningDataTaskList  server name
	redisKeyRunningDataTaskList = "Titan:RunningDataTaskList:%s"
	// redisKeyRunningDataTask  server name:cid
	redisKeyRunningDataTask = "Titan:RunningDataTask:%s:%s"
	// redisKeyCacheResult  server name
	redisKeyCacheResult = "Titan:CacheResult:%s"
	// redisKeyNodeBlockFid  deviceID
	redisKeyNodeBlockFid = "Titan:NodeBlockFid:%s"
	// redisKeyValidatorList server name
	redisKeyValidatorList = "Titan:ValidatorList:%s"
	// redisKeyValidateRoundID server name
	redisKeyValidateRoundID = "Titan:ValidateRoundID:%s"
	// redisKeyValidateingList server name
	redisKeyValidateingList = "Titan:ValidateingList:%s"
	// redisKeyNodeInfo  deviceID
	redisKeyNodeInfo = "Titan:NodeInfo:%s"
	// redisKeyBlockDownloadRecord serial number
	redisKeyBlockDownloadRecord = "Titan:BlockDownloadRecord:%d"
	// redisKeyBlockDownloadSN
	redisKeyBlockDownloadSN       = "Titan:BlockDownloadRecordSN"
	redisKeyCarfileLatestDownload = "Titan:LatestDownload:%s"

	// NodeInfo field
	onlineTimeField             = "OnlineTime"
	validateSuccessField        = "ValidateSuccessTime"
	nodeTodayRewardField        = "TodayProfit"
	nodeRewardDateTimeField     = "RewardDateTime"
	nodeLatencyField            = "Latency"
	blockDownloadCIDField       = "CID"
	blockDownloadPublicKeyField = "UserPublicKey"

	// redisKeySystemInfo  server name
	redisKeySystemInfo = "Titan:BaseInfo:%s"
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

func (rd redisDB) IncrNodeOnlineTime(deviceID string, onlineTime float64) (float64, error) {
	key := fmt.Sprintf(redisKeyNodeInfo, deviceID)

	return rd.cli.HIncrByFloat(context.Background(), key, onlineTimeField, onlineTime).Result()
}

func (rd redisDB) IncrNodeValidateTime(deviceID string, validateSuccessTime int64) (int64, error) {
	key := fmt.Sprintf(redisKeyNodeInfo, deviceID)

	return rd.cli.HIncrBy(context.Background(), key, validateSuccessField, validateSuccessTime).Result()
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

func (rd redisDB) GetPreviousAndCurrentRoundId() (pre, cur int64, err error) {
	key := fmt.Sprintf(redisKeyValidateRoundID, serverName)
	cur, err = rd.cli.IncrBy(context.Background(), key, 1).Result()
	if err != nil {
		return
	}
	pre = cur - 1
	return
}

// get ValidateID
func (rd redisDB) GetValidateRoundID() (string, error) {
	key := fmt.Sprintf(redisKeyValidateRoundID, serverName)

	return rd.cli.Get(context.Background(), key).Result()
}

// add
func (rd redisDB) SetNodeToVerifyingList(deviceID string) error {
	key := fmt.Sprintf(redisKeyValidateingList, serverName)

	_, err := rd.cli.SAdd(context.Background(), key, deviceID).Result()
	return err
}

// SMembers
func (rd redisDB) GetNodesWithVerifyingList() ([]string, error) {
	key := fmt.Sprintf(redisKeyValidateingList, serverName)

	return rd.cli.SMembers(context.Background(), key).Result()
}

func (rd redisDB) CountVerifyingNode(ctx context.Context) (int64, error) {
	key := fmt.Sprintf(redisKeyValidateingList, serverName)
	return rd.cli.SCard(ctx, key).Result()
}

// del device
func (rd redisDB) RemoveNodeWithVerifyingList(deviceID string) error {
	key := fmt.Sprintf(redisKeyValidateingList, serverName)

	_, err := rd.cli.SRem(context.Background(), key, deviceID).Result()
	return err
}

// del key
func (rd redisDB) RemoveVerifyingList() error {
	key := fmt.Sprintf(redisKeyValidateingList, serverName)

	_, err := rd.cli.Del(context.Background(), key).Result()
	return err
}

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

func (rd redisDB) SetDeviceInfo(deviceID string, info api.DevicesInfo) (bool, error) {
	key := fmt.Sprintf(redisKeyNodeInfo, deviceID)

	ctx := context.Background()
	exist, err := rd.cli.Exists(ctx, key).Result()
	if err != nil {
		return false, err
	}

	if exist == 1 {
		return true, rd.updateBaseDeviceInfo(deviceID, info)
	}

	_, err = rd.cli.Pipelined(ctx, func(pipeline redis.Pipeliner) error {
		for field, value := range toMap(info) {
			pipeline.HMSet(ctx, key, field, value)
		}
		return nil
	})
	if err != nil {
		return false, err
	}

	return true, nil
}

func (rd redisDB) updateBaseDeviceInfo(deviceID string, info api.DevicesInfo) error {
	return rd.UpdateDeviceInfo(deviceID, func(deviceInfo *api.DevicesInfo) {
		deviceInfo.NodeType = info.NodeType
		deviceInfo.DeviceName = info.DeviceName
		deviceInfo.SnCode = info.SnCode
		deviceInfo.Operator = info.Operator
		deviceInfo.NetworkType = info.NetworkType
		deviceInfo.SystemVersion = info.SystemVersion
		deviceInfo.ProductType = info.ProductType
		deviceInfo.NetworkInfo = info.NetworkInfo
		deviceInfo.ExternalIp = info.ExternalIp
		deviceInfo.InternalIp = info.InternalIp
		deviceInfo.IpLocation = info.IpLocation
		deviceInfo.MacLocation = info.MacLocation
		deviceInfo.NatType = info.NatType
		deviceInfo.Upnp = info.Upnp
		deviceInfo.CpuUsage = info.CpuUsage
		deviceInfo.CPUCores = info.CPUCores
		deviceInfo.MemoryUsage = info.MemoryUsage
		deviceInfo.Memory = info.Memory
		deviceInfo.DiskUsage = info.DiskUsage
		deviceInfo.DiskSpace = info.DiskSpace
		deviceInfo.DiskType = info.DiskType
		deviceInfo.DeviceStatus = info.DeviceStatus
		deviceInfo.WorkStatus = info.WorkStatus
		deviceInfo.IoSystem = info.IoSystem
		deviceInfo.BandwidthUp = info.BandwidthUp
		deviceInfo.BandwidthDown = info.BandwidthDown
	})
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

	// value, err := rd.cli.LIndex(context.Background(), key, 0).Result()
	value, err := rd.cli.LPop(context.Background(), key).Result()

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

func (rd redisDB) SetRunningDataTask(hash, cacheID string, timeout int64) error {
	key := fmt.Sprintf(redisKeyRunningDataTask, serverName, hash)
	// Expire
	_, err := rd.cli.Set(context.Background(), key, cacheID, time.Second*time.Duration(timeout)).Result()
	return err
}

func (rd redisDB) GetRunningDataTaskExpiredTime(hash string) (time.Duration, error) {
	key := fmt.Sprintf(redisKeyRunningDataTask, serverName, hash)
	// Expire
	return rd.cli.TTL(context.Background(), key).Result()
}

func (rd redisDB) GetRunningDataTask(hash string) (string, error) {
	key := fmt.Sprintf(redisKeyRunningDataTask, serverName, hash)

	return rd.cli.Get(context.Background(), key).Result()
}

func (rd redisDB) RemoveRunningDataTask(hash, cacheID string) error {
	key := fmt.Sprintf(redisKeyRunningDataTask, serverName, hash)

	_, err := rd.cli.Del(context.Background(), key).Result()
	if err != nil {
		return err
	}

	return rd.RemoveDataTaskWithRunningList(hash, cacheID)
}

func (rd redisDB) SetWaitingDataTask(info api.DataInfo) error {
	key := fmt.Sprintf(redisKeyWaitingDataTaskList, serverName)

	bytes, err := json.Marshal(info)
	if err != nil {
		return err
	}

	_, err = rd.cli.RPush(context.Background(), key, bytes).Result()
	return err
}

func (rd redisDB) GetWaitingDataTask(index int64) (api.DataInfo, error) {
	key := fmt.Sprintf(redisKeyWaitingDataTaskList, serverName)

	value, err := rd.cli.LIndex(context.Background(), key, index).Result()

	if value == "" {
		return api.DataInfo{}, redis.Nil
	}

	var info api.DataInfo
	bytes, err := redigo.Bytes(value, nil)
	if err != nil {
		return api.DataInfo{}, err
	}

	if err := json.Unmarshal(bytes, &info); err != nil {
		return api.DataInfo{}, err
	}

	return info, nil
}

func (rd redisDB) RemoveWaitingDataTask(info api.DataInfo) error {
	key := fmt.Sprintf(redisKeyWaitingDataTaskList, serverName)

	bytes, err := json.Marshal(info)
	if err != nil {
		return err
	}
	_, err = rd.cli.LRem(context.Background(), key, 1, bytes).Result()
	// _, err := rd.cli.LPop(context.Background(), key).Result()
	return err
}

// add
func (rd redisDB) SetDataTaskToRunningList(hash, cacheID string) error {
	key := fmt.Sprintf(redisKeyRunningDataTaskList, serverName)

	info := DataTask{CarfileHash: hash, CacheID: cacheID}
	bytes, err := json.Marshal(info)
	if err != nil {
		return err
	}

	_, err = rd.cli.SAdd(context.Background(), key, bytes).Result()
	return err
}

// del
func (rd redisDB) RemoveDataTaskWithRunningList(hash, cacheID string) error {
	key := fmt.Sprintf(redisKeyRunningDataTaskList, serverName)

	info := DataTask{CarfileHash: hash, CacheID: cacheID}
	bytes, err := json.Marshal(info)
	if err != nil {
		return err
	}

	_, err = rd.cli.SRem(context.Background(), key, bytes).Result()
	return err
}

// SMembers
func (rd redisDB) GetDataTasksWithRunningList() ([]DataTask, error) {
	key := fmt.Sprintf(redisKeyRunningDataTaskList, serverName)

	values, err := rd.cli.SMembers(context.Background(), key).Result()
	if err != nil || values == nil {
		return nil, err
	}

	list := make([]DataTask, 0)
	for _, value := range values {
		var info DataTask
		bytes, err := redigo.Bytes(value, nil)
		if err != nil {
			continue
		}

		if err := json.Unmarshal(bytes, &info); err != nil {
			continue
		}

		list = append(list, info)
	}

	return list, nil
}

func (rd redisDB) SetDownloadBlockRecord(record DownloadBlockRecord) error {
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

func (rd redisDB) GetDownloadBlockRecord(sn int64) (DownloadBlockRecord, error) {
	key := fmt.Sprintf(redisKeyBlockDownloadRecord, sn)

	var record DownloadBlockRecord
	err := rd.cli.HGetAll(context.Background(), key).Scan(&record)
	if err != nil {
		return DownloadBlockRecord{}, err
	}

	return record, nil
}

func (rd redisDB) RemoveDownloadBlockRecord(sn int64) error {
	key := fmt.Sprintf(redisKeyBlockDownloadRecord, sn)
	_, err := rd.cli.Del(context.Background(), key).Result()
	return err
}

// node cache tag ++1
func (rd redisDB) IncrBlockDownloadSN() (int64, error) {
	n, err := rd.cli.IncrBy(context.Background(), redisKeyBlockDownloadSN, 1).Result()
	if n >= math.MaxInt64 {
		rd.cli.Set(context.Background(), redisKeyBlockDownloadSN, 0, 0).Result()
	}

	return n, err
}

func (rd redisDB) UpdateDeviceInfo(deviceID string, update func(deviceInfo *api.DevicesInfo)) error {
	key := fmt.Sprintf(redisKeyNodeInfo, deviceID)
	deviceInfo, err := rd.GetDeviceInfo(deviceID)
	if err != nil {
		return err
	}

	update(&deviceInfo)

	ctx := context.Background()
	_, err = rd.cli.Pipelined(ctx, func(pipeliner redis.Pipeliner) error {
		for field, value := range toMap(deviceInfo) {
			pipeliner.HMSet(ctx, key, field, value)
		}
		return nil
	})

	return err
}

func (rd redisDB) UpdateSystemInfo(update func(info *api.BaseInfo)) error {
	key := fmt.Sprintf(redisKeySystemInfo, serverName)
	info, err := rd.GetSystemInfo()
	if err != nil {
		return err
	}

	update(&info)

	ctx := context.Background()
	_, err = rd.cli.Pipelined(ctx, func(pipeliner redis.Pipeliner) error {
		for field, value := range systemInfoToMap(info) {
			pipeliner.HMSet(ctx, key, field, value)
		}
		return nil
	})

	return err
}

func (rd redisDB) GetSystemInfo() (api.BaseInfo, error) {
	key := fmt.Sprintf(redisKeySystemInfo, serverName)

	var info api.BaseInfo
	err := rd.cli.HGetAll(context.Background(), key).Scan(&info)
	if err != nil {
		return api.BaseInfo{}, err
	}

	return info, nil
}

func systemInfoToMap(info api.BaseInfo) map[string]interface{} {
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
