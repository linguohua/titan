package cache

import (
	"context"
	"time"

	"github.com/linguohua/titan/api"
	"golang.org/x/xerrors"
)

const (
	//CarFileCountField BaseInfo Field
	CarFileCountField = "CarfileCount"
	//DownloadCountField BaseInfo Field
	DownloadCountField = "DownloadCount"
	//NextElectionTimeField BaseInfo Field
	NextElectionTimeField = "NextElectionTime"

	//BlockCountField DeviceInfo Field
	BlockCountField = "BlockCount"
	//TotalDownloadField DeviceInfo Field
	TotalDownloadField = "TotalDownload"
	//TotalUploadField DeviceInfo Field
	TotalUploadField = "TotalUpload"
	//DiskUsageField DeviceInfo Field
	DiskUsageField = "DiskUsage"
)

// DB cache db
type DB interface {
	// cache result info
	SetCacheResultInfo(info *api.CacheResultInfo) (int64, error)
	GetCacheResultInfo() (*api.CacheResultInfo, error)
	GetCacheResultNum() int64

	// running data list
	SetDataTaskToRunningList(hash, cacheID string, timeout int64) error
	GetDataTasksWithRunningList() ([]*DataTask, error)

	// running data details
	SetRunningDataTask(hash, deviceID string, timeout int64) error
	GetRunningDataTask(hash, deviceID string) (string, error)
	RemoveRunningDataTask(hash, deviceID string) error
	GetRunningDataTaskExpiredTime(hash, deviceID string) (time.Duration, error)

	// waiting data list
	SetWaitingDataTask(info *api.CarfileRecordInfo) error
	GetWaitingDataTask(index int64) (*api.CarfileRecordInfo, error)
	RemoveWaitingDataTasks(infos []*api.CarfileRecordInfo) error

	// validate round id
	IncrValidateRoundID() (int64, error)

	// verifying node list
	SetNodeToVerifyingList(deviceID string) error
	RemoveNodeWithVerifyingList(deviceID string) error
	RemoveVerifyingList() error
	GetNodesWithVerifyingList() ([]string, error)
	CountVerifyingNode(ctx context.Context) (int64, error)

	// validator list
	SetValidatorsToList(deviceIDs []string, expiration time.Duration) error
	GetValidatorsWithList() ([]string, error)
	GetValidatorsAndExpirationTime() ([]string, time.Duration, error)

	// device info
	IncrNodeOnlineTime(deviceID string, onlineTime int64) (float64, error)
	SetDeviceInfo(info *api.DevicesInfo) error
	GetDeviceInfo(deviceID string) (*api.DevicesInfo, error)
	UpdateDeviceInfo(deviceID, field string, value interface{}) error
	UpdateDevicesInfo(field string, values map[string]interface{}) error
	IncrByDeviceInfo(deviceID, field string, value int64) error
	IncrByDevicesInfo(field string, values map[string]int64) error
	UpdateNodeCacheBlockInfo(toDeviceID, fromDeviceID string, blockSize int) error

	// download info
	SetDownloadBlockRecord(record *DownloadBlockRecord) error
	RemoveDownloadBlockRecord(sn int64) error
	GetDownloadBlockRecord(sn int64) (*DownloadBlockRecord, error)
	IncrBlockDownloadSN() (int64, error)

	// latest data of download
	AddLatestDownloadCarfile(carfileCID string, userIP string) error
	GetLatestDownloadCarfiles(userIP string) ([]string, error)

	// cache error details
	SaveCacheErrors(cacheID string, infos []*api.CacheError, isClean bool) error
	GetCacheErrors(cacheID string) ([]*api.CacheError, error)

	NodeDownloadCount(deviceID string, blockDownnloadInfo *api.BlockDownloadInfo) error

	// system base info TODO save in db
	GetBaseInfo() (*api.BaseInfo, error)
	UpdateBaseInfo(field string, value interface{}) error
	IncrByBaseInfo(field string, value int64) error

	// node fid TODO save in db
	IncrNodeCacheFid(deviceID string, num int) (int, error)
	GetNodeCacheFid(deviceID string) (int64, error)

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

// // NodeInfo base info
// type NodeInfo struct {
// 	OnLineTime int64
// 	LastTime   string
// 	Geo        string
// 	IsOnline   bool
// 	NodeType   api.NodeTypeName
// }

// DataTask data cache task
type DataTask struct {
	CarfileHash string
	DeviceID    string
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
