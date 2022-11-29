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

	SetDataTaskToRunningList(hash, cacheID string) error
	RemoveDataTaskWithRunningList(hash, cacheID string) error
	GetDataTasksWithRunningList() ([]DataTask, error)

	SetRunningDataTask(hash, cacheID string, timeout int64) error
	GetRunningDataTask(hash string) (string, error)
	RemoveRunningDataTask(hash, cacheID string) error

	SetWaitingDataTask(info api.CacheDataInfo) error
	GetWaitingDataTask(index int64) (api.CacheDataInfo, error)
	RemoveWaitingDataTask(info api.CacheDataInfo) error

	IncrNodeCacheFid(deviceID string, num int) (int, error)
	GetNodeCacheFid(deviceID string) (int64, error)

	IncrValidateRoundID() (int64, error)
	GetValidateRoundID() (string, error)

	RemoveNodeWithValidateingList(deviceID string) error
	SetNodeToValidateingList(deviceID string) error
	GetNodesWithValidateingList() ([]string, error)
	RemoveValidateingList() error

	SetValidatorToList(deviceID string) error
	GetValidatorsWithList() ([]string, error)
	RemoveValidatorList() error

	IncrNodeOnlineTime(deviceID string, onlineTime float64) (float64, error)
	IncrNodeValidateTime(deviceID string, validateSuccessTime int64) (int64, error)

	GetDeviceStat() (api.StateNetwork, error)
	SetDeviceInfo(deviceID string, info api.DevicesInfo) (bool, error)
	GetDeviceInfo(deviceID string) (api.DevicesInfo, error)
	IncrDeviceReward(deviceID string, reward int64) error
	SetDeviceLatency(deviceID string, latency float64) error
	SetDownloadBlockRecord(record DownloadBlockRecord) error
	GetDownloadBlockRecord(sn int64) (DownloadBlockRecord, error)
	IncrBlockDownloadSN() (int64, error)

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

// DataTask data cache task
type DataTask struct {
	CarfileHash string
	CacheID     string
}

type DownloadBlockRecord struct {
	SN            int64  `redis:"-"`
	Cid           string `redis:"Cid"`
	UserPublicKey string `redis:"UserPublicKey"`
	NodeStatus    int    `redis:"NodeStatus"`
	UserStatus    int    `redis:"UserStatus"`
	SignTime      int64  `redis:"SignTime"`
	Timeout       int    `redis:"Timeout"`
}
