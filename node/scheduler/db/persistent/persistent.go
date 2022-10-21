package persistent

import (
	"github.com/linguohua/titan/api"
	"golang.org/x/xerrors"
)

// DB Persistent db
type DB interface {
	IsNilErr(err error) bool

	// Node Info
	SetNodeInfo(deviceID string, info *NodeInfo) error
	// AddNodeOnlineTime(deviceID string, onLineTime int64) error
	GetNodeInfo(deviceID string) (*NodeInfo, error)
	// AddAllNodeOnlineTime(onLineTime int64) error
	SetAllNodeOffline() error

	// Validate Result
	SetValidateResultInfo(info *ValidateResult) error
	SetNodeToValidateErrorList(sID, deviceID string) error

	RemoveBlockInfo(deviceID, cid string) error
	SetBlockInfo(deviceID, cid string, fid string, isUpdate bool) error
	SetCarfileInfo(deviceID, cid, carfileID, cacheID string) error
	GetBlockFidWithCid(deviceID, cid string) (string, error)
	GetBlockInfos(deviceID string) (map[string]string, error)
	GetBlockCidWithFid(deviceID, fid string) (string, error)
	GetBlockNum(deviceID string) (int64, error)

	// data info
	SetDataInfo(info *DataInfo) error
	GetDataInfo(cid string) (*DataInfo, error)
	GetDataInfos() ([]*DataInfo, error)
	// cache info
	CreateCacheInfo(cacheID string) error
	SetCacheInfo(info *CacheInfo, isUpdate bool) error
	GetCacheInfo(cacheID, cid string) (*CacheInfo, error)
	GetCacheInfos(cacheID string) ([]*CacheInfo, error)
	HaveUndoneCaches(cacheID string) (bool, error)

	// temporary node register
	BindRegisterInfo(secret, deviceID string, nodeType api.NodeType) error
	GetRegisterInfo(deviceID string) (*api.NodeRegisterInfo, error)
}

var (
	db DB

	serverName string
)

// NewDB New  DB
func NewDB(url, dbType, sName string) error {
	var err error

	serverName = sName

	switch dbType {
	case TypeSQL():
		db, err = InitSQL(url)
	default:
		// panic("unknown DB type")
		err = xerrors.New("unknown DB type")
	}

	// if err != nil {
	// 	eStr = fmt.Sprintf("NewDB err:%v , url:%v", err.Error(), url)
	// 	// panic(e)
	// }

	return err
}

// GetDB Get DB
func GetDB() DB {
	return db
}

// NodeInfo base info
type NodeInfo struct {
	ID         int
	DeviceID   string `db:"device_id"`
	LastTime   string `db:"last_time"`
	OnlineTime int64  `db:"online_time"`
	Geo        string `db:"geo"`
	IsOnline   int    `db:"is_online"`
	NodeType   string `db:"node_type"`
	Address    string `db:"address"`
	ServerName string `db:"server_name"`
	CreateTime string `db:"create_time"`
}

// ValidateResult validate result
type ValidateResult struct {
	ID          int
	RoundID     string `db:"round_id"`
	DeviceID    string `db:"device_id"`
	ValidatorID string `db:"validator_id"`
	Msg         string `db:"msg"`
	Status      int    `db:"status"`
	StratTime   string `db:"strat_time"`
	EndTime     string `db:"end_time"`
	ServerName  string `db:"server_name"`
}

// NodeBlock Node Block
type NodeBlock struct {
	TableName string `db:"table_name"`
	DeviceID  string `db:"device_id"`
	FID       string `db:"fid"`
	CID       string `db:"cid"`
	CarfileID string `db:"carfile_id"`
	CacheID   string `db:"cache_id"`
}

// DataInfo Data info
type DataInfo struct {
	CID             string `db:"cid"`
	CacheIDs        string `db:"cache_ids"`
	Status          int    `db:"status"`
	TotalSize       int    `db:"total_size"`
	Reliability     int    `db:"reliability"`
	NeedReliability int    `db:"need_reliability"`
}

// CacheInfo Data Cache info
type CacheInfo struct {
	CacheID     string `db:"cache_id"`
	CID         string `db:"cid"`
	DeviceID    string `db:"device_id"`
	Status      int    `db:"status"`
	TotalSize   int    `db:"total_size"`
	Reliability int    `db:"reliability"`
}

// ValidateStatus Validate Status
type ValidateStatus int

const (
	// ValidateStatusUnknown status
	ValidateStatusUnknown ValidateStatus = iota
	// ValidateStatusCreate status
	ValidateStatusCreate
	// ValidateStatusTimeOut status
	ValidateStatusTimeOut
	// ValidateStatusSuccess status
	ValidateStatusSuccess
	// ValidateStatusFail status
	ValidateStatusFail
	// ValidateStatusOther status
	ValidateStatusOther
)
