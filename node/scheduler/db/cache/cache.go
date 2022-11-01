package cache

import (
	"github.com/linguohua/titan/api"
	"golang.org/x/xerrors"
)

// DB cache db
type DB interface {
	SetCacheResultInfo(info api.CacheResultInfo) error
	GetCacheResultInfo() (api.CacheResultInfo, error)
	RemoveCacheResultInfo() error
	GetCacheResultNum() int64

	SetCidToRunningList(cid string) error
	RemoveRunningList(cid string) error
	IsCidInRunningList(cid string) (bool, error)

	// SetRunningCacheTask(cid string) error
	// GetRunningCacheTask() (string, error)
	// RemoveRunningCacheTask() error

	SetWaitingCacheTask(info api.CacheDataInfo) error
	GetWaitingCacheTask() (api.CacheDataInfo, error)
	RemoveWaitingCacheTask() error

	// IncrDataCacheKey(cacheID string) (int64, error)
	// DeleteDataCache(cacheID string) error

	IncrNodeCacheFid(deviceID string, num int) (int, error)
	GetNodeCacheFid(deviceID string) (int64, error)

	IncrValidateRoundID() (int64, error)
	GetValidateRoundID() (string, error)

	RemoveNodeWithValidateingList(deviceID string) error
	SetNodeToValidateingList(deviceID string) error
	GetNodesWithValidateingList() ([]string, error)
	RemoveValidateingList() error

	// RemoveNodeWithCacheList(deviceID, cid string) error
	// SetNodeToCacheList(deviceID, cid string) error
	// GetNodesWithCacheList(cid string) ([]string, error)

	SetValidatorToList(deviceID string) error
	GetValidatorsWithList() ([]string, error)
	RemoveValidatorList() error
	IsNodeInValidatorList(deviceID string) (bool, error)

	IncrNodeOnlineTime(deviceID string, onlineTime float64) (float64, error)
	IncrNodeValidateTime(deviceID string, validateSuccessTime int64) (int64, error)

	IncrCacheID(area string) (int64, error)

	// SetCacheDataTask(cid, cacheID string) error
	// RemoveCacheDataTask() error
	// GetCacheDataTask() (string, string)

	IncrNodeReward(deviceID string, reward int64) error
	GetNodeReward(deviceID string) (int64, int64, int64, error)

	SetDeviceInfo(deviceID string, info api.DevicesInfo) (bool, error)
	GetDeviceInfo(deviceID string) (api.DevicesInfo, error)

	IsNilErr(err error) bool
}

var (
	db         DB
	serverName string
)

// NewCacheDB New Cache DB
func NewCacheDB(url, dbType, sName string) error {
	var err error

	serverName = sName

	switch dbType {
	case TypeRedis():
		db, err = InitRedis(url)
	default:
		// panic("unknown CacheDB type")
		err = xerrors.New("unknown CacheDB type")
	}

	// if err != nil {
	// 	eStr = fmt.Sprintf("NewCacheDB err:%v , url:%v", err.Error(), url)
	// 	// panic(e)
	// }

	return err
}

// GetDB Get CacheDB
func GetDB() DB {
	return db
}

// NodeInfo base info
type NodeInfo struct {
	OnLineTime int64
	LastTime   string
	Geo        string
	IsOnline   bool
	NodeType   api.NodeTypeName
}
