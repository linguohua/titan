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
	GetNodeInfo(deviceID string) (*NodeInfo, error)
	SetAllNodeOffline() error
	// AddAllNodeOnlineTime(onLineTime int64) error
	// AddNodeOnlineTime(deviceID string, onLineTime int64) error

	// Validate Result
	SetValidateResultInfo(info *ValidateResult) error
	SetNodeToValidateErrorList(sID, deviceID string) error

	// data info
	SetDataInfo(area string, info *DataInfo) error
	GetDataInfo(area, cid string) (*DataInfo, error)
	GetDataInfos(area string) ([]*DataInfo, error)

	// cache info
	SetCacheInfos(area string, infos []*BlockInfo, isUpdate bool) error
	SetCacheInfo(area string, info *BlockInfo) error
	GetCacheInfo2(area string, id int) (*BlockInfo, error)
	GetCacheInfo(area, cacheID, cid string) (*BlockInfo, error)
	GetCacheInfos(area, cacheID string) ([]*BlockInfo, error)
	HaveUndoneCaches(area, cacheID string) (bool, error)
	GetUndoneCaches(area, cacheID string) ([]string, error)

	// node block
	DeleteBlockInfo(area, deviceID, cid string) error
	AddBlockInfo(area, deviceID, cid, fid, carfileID, cacheID string) error
	GetBlockFidWithCid(area, deviceID, cid string) (string, error)
	GetBlockInfos(area, deviceID string) (map[string]string, error)
	GetBlockNum(area, deviceID string) (int64, error)
	GetNodesWithCacheList(area, cid string) ([]string, error)

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
	OnlineTime int64  `db:"online_time"` // 废弃
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

// NodeBlocks Node Block
type NodeBlocks struct {
	ID        int
	DeviceID  string `db:"device_id"`
	FID       string `db:"fid"`
	CID       string `db:"cid"`
	CarfileID string `db:"carfile_id"`
	CacheID   string `db:"cache_id"`
}

// BlockNodes Node Block
type BlockNodes struct {
	ID       int
	DeviceID string `db:"device_id"`
	CID      string `db:"cid"`
}

// DataInfo Data info
type DataInfo struct {
	CID             string `db:"cid"`
	CacheIDs        string `db:"cache_ids"`
	Status          int    `db:"status"`
	TotalSize       int    `db:"total_size"`
	Reliability     int    `db:"reliability"`
	NeedReliability int    `db:"need_reliability"`
	CacheTime       int    `db:"cache_time"`
	RootCacheID     string `db:"root_cache_id"`
	TotalBlocks     int    `db:"total_blocks"`
}

// BlockInfo Data Block info
type BlockInfo struct {
	ID          int
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
