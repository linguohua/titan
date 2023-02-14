package cache

import (
	"time"

	"github.com/linguohua/titan/api"
	"golang.org/x/xerrors"
)

const (
	// CarFileCountField SystemBaseInfo Field
	CarFileCountField = "CarfileCount"
	// DownloadCountField SystemBaseInfo Field
	DownloadCountField = "DownloadCount"
	// NextElectionTimeField SystemBaseInfo Field
	NextElectionTimeField = "NextElectionTime"

	// blockCountField DeviceInfo Field
	blockCountField = "BlockCount"
	// totalDownloadField DeviceInfo Field
	totalDownloadField = "TotalDownload"
	// totalUploadField DeviceInfo Field
	totalUploadField = "TotalUpload"
	// diskUsageField DeviceInfo Field
	diskUsageField = "DiskUsage"
)

// DB cache db
type DB interface {
	CacheTasksStart(hash string, deviceIDs []string, timeout int64) error
	CacheTasksEnd(hash string, deviceIDs []string) (bool, error)
	UpdateNodeCacheingExpireTime(hash, deviceID string, timeout int64) error
	GetCacheingCarfiles() ([]string, error)
	GetNodeCacheTimeoutTime(deviceID string) (int, error)

	// carfile record cache result
	SetCarfileRecordCacheResult(hash string, info *api.CarfileRecordCacheResult) error
	GetCarfileRecordCacheResult(hash string) (*api.CarfileRecordCacheResult, error)

	// waiting data list
	PushCarfileToWaitList(info *api.CacheCarfileInfo) error
	GetWaitCarfile() (*api.CacheCarfileInfo, error)

	// validate round id
	IncrValidateRoundID() (int64, error)

	// verifying node list
	SetNodesToVerifyingList(deviceIDs []string) error
	RemoveValidatedWithList(deviceID string) error
	CountVerifyingNode() (int64, error)
	RemoveVerifyingList() error
	GetNodesWithVerifyingList() ([]string, error)
	// CountVerifyingNode(ctx context.Context) (int64, error)

	// validator list
	SetValidatorsToList(deviceIDs []string, expiration time.Duration) error
	GetValidatorsWithList() ([]string, error)
	GetValidatorsAndExpirationTime() ([]string, time.Duration, error)

	// device info
	IncrNodeOnlineTime(deviceID string, onlineTime int64) (float64, error)
	SetDeviceInfo(info *api.DevicesInfo) error
	GetDeviceInfo(deviceID string) (*api.DevicesInfo, error)
	IncrByDeviceInfo(deviceID, field string, value int64) error

	// download info
	SetDownloadBlockRecord(record *DownloadBlockRecord) error
	RemoveDownloadBlockRecord(sn int64) error
	GetDownloadBlockRecord(sn int64) (*DownloadBlockRecord, error)
	IncrBlockDownloadSN() (int64, error)

	// latest data of download
	AddLatestDownloadCarfile(carfileCID string, userIP string) error
	GetLatestDownloadCarfiles(userIP string) ([]string, error)

	NodeDownloadCount(deviceID string, blockDownnloadInfo *api.BlockDownloadInfo) error

	// system base info
	GetSystemBaseInfo() (*api.SystemBaseInfo, error)
	UpdateSystemBaseInfo(field string, value interface{}) error
	IncrBySystemBaseInfo(field string, value int64) error
	UpdateNodeCacheInfo(deviceID string, nodeInfo *NodeCacheInfo) error

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
		err = xerrors.New("unknown CacheDB type")
	}

	return err
}

// GetDB Get CacheDB
func GetDB() DB {
	return db
}

// DataTask data cache task
type DataTask struct {
	CarfileHash string
	DeviceID    string
}

// NodeCacheInfo node cache info
type NodeCacheInfo struct {
	DiskUsage     float64
	TotalDownload float64
	TotalUpload   float64
	BlockCount    int
	DeviceID      string
}

type DownloadBlockRecord struct {
	SN            int64  `redis:"SN"`
	ID            string `redis:"ID"`
	Cid           string `redis:"Cid"`
	UserPublicKey string `redis:"UserPublicKey"`
	NodeStatus    int    `redis:"NodeStatus"`
	UserStatus    int    `redis:"UserStatus"`
	SignTime      int64  `redis:"SignTime"`
	Timeout       int    `redis:"Timeout"`
}
