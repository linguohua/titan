package persistent

import (
	"time"

	"github.com/linguohua/titan/api"
	"golang.org/x/xerrors"
)

// DB Persistent db
type DB interface {
	IsNilErr(err error) bool

	// Node Info
	SetNodeInfo(deviceID string, info *NodeInfo) error
	GetNodeInfo(deviceID string) (*NodeInfo, error)
	SetNodeAuthInfo(info *api.DownloadServerAccessAuth) error
	GetNodeAuthInfo(deviceID string) (*api.DownloadServerAccessAuth, error)

	// Validate Result
	SetValidateResultInfo(info *ValidateResult) error
	SetNodeToValidateErrorList(sID, deviceID string) error

	CreateCache(cInfo *CacheInfo) error
	SaveCacheEndResults(dInfo *DataInfo, cInfo *CacheInfo) error
	SaveCacheingResults(dInfo *DataInfo, cInfo *CacheInfo, updateBlock *BlockInfo, createBlocks []*BlockInfo) error

	// data info
	SetDataInfo(info *DataInfo) error
	GetDataInfo(hash string) (*DataInfo, error)
	GetDataCidWithPage(page int) (count int, totalPage int, list []string, err error)
	GetCachesWithData(hash string) ([]string, error)

	ChangeExpiredTimeWhitCaches(carfileHash string, time time.Time) error
	GetExpiredCaches() ([]*CacheInfo, error)
	// cache info
	GetCacheInfo(cacheID string) (*CacheInfo, error)
	RemoveCacheInfo(cacheID, carfileHash string, isDeleteData bool, reliability int) error
	GetCachesSize(cacheID string, status int) (int, error)

	// block info
	// SetBlockInfos(infos []*BlockInfo, carfileCid string) error
	GetBlockInfo(cacheID, hash, deviceID string) (*BlockInfo, error)
	GetBloackCountWithStatus(cacheID string, status int) (int, error)
	GetUndoneBlocks(cacheID string) (map[string]string, error)
	GetAllBlocks(cacheID string) ([]*BlockInfo, error)
	GetNodesFromCache(cacheID string) (int, error)
	GetNodesFromData(hash string) (int, error)

	// node block
	// DeleteBlockInfos(cacheID, deviceID string, cids []string, removeBlocks int) error
	GetBlocksFID(deviceID string) (map[int]string, error)
	GetBlocksInRange(startFid, endFid int, deviceID string) (map[int]string, error)
	GetBlocksBiggerThan(startFid int, deviceID string) (map[int]string, error)
	GetDeviceBlockNum(deviceID string) (int64, error)
	GetNodesWithCache(hash string, isSuccess bool) ([]string, error)
	// GetNodeBlock(deviceID, cid string) ([]*BlockInfo, error)

	// temporary node register
	BindRegisterInfo(secret, deviceID string, nodeType api.NodeType) error
	GetRegisterInfo(deviceID string) (*api.NodeRegisterInfo, error)

	// AddDownloadInfo user download block information
	AddDownloadInfo(info *api.BlockDownloadInfo) error
	GetDownloadInfoByDeviceID(deviceID string) ([]*api.BlockDownloadInfo, error)
	GetDownloadInfoBySN(sn int64) (*api.BlockDownloadInfo, error)

	SetEventInfo(info *api.EventInfo) error
	GetEventInfos(page int) (count int, totalPage int, out []api.EventInfo, err error)
	// AddToBeDeleteBlock(infos []*BlockDelete) error
	// RemoveToBeDeleteBlock(infos []*BlockDelete) error
	// GetToBeDeleteBlocks(deviceID string) ([]*BlockDelete, error)
	SetMessageInfo(infos []*MessageInfo) error

	// tool
	ReplaceArea() string

	// web
	webDB
}

var (
	db DB

	serverName string
	serverArea string
)

// NewDB New  DB
func NewDB(url, dbType, sName, sArea string) error {
	var err error

	serverName = sName
	serverArea = sArea

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
	DeviceID   string    `db:"device_id"`
	LastTime   string    `db:"last_time"`
	Geo        string    `db:"geo"`
	IsOnline   int       `db:"is_online"`
	NodeType   string    `db:"node_type"`
	Address    string    `db:"address"`
	ServerName string    `db:"server_name"`
	CreateTime time.Time `db:"create_time"`
	URL        string    `db:"url"`
	PrivateKey string    `db:"private_key"`
}

// ValidateResult validate result
type ValidateResult struct {
	ID          int
	RoundID     string `db:"round_id"`
	DeviceID    string `db:"device_id"`
	ValidatorID string `db:"validator_id"`
	Msg         string `db:"msg"`
	Status      int    `db:"status"`
	StartTime   string `db:"start_time"`
	EndTime     string `db:"end_time"`
	ServerName  string `db:"server_name"`
}

// DataInfo Data info
type DataInfo struct {
	CarfileCid      string    `db:"carfile_cid"`
	CarfileHash     string    `db:"carfile_hash"`
	Status          int       `db:"status"`
	Reliability     int       `db:"reliability"`
	NeedReliability int       `db:"need_reliability"`
	CacheCount      int       `db:"cache_count"`
	TotalSize       int       `db:"total_size"`
	TotalBlocks     int       `db:"total_blocks"`
	Nodes           int       `db:"nodes"`
	ExpiredTime     time.Time `db:"expired_time"`
	CreateTime      time.Time `db:"created_time"`
	EndTime         time.Time `db:"end_time"`
}

// CacheInfo Data Block info
type CacheInfo struct {
	CarfileHash string    `db:"carfile_hash"`
	CacheID     string    `db:"cache_id"`
	Status      int       `db:"status"`
	Reliability int       `db:"reliability"`
	DoneSize    int       `db:"done_size"`
	DoneBlocks  int       `db:"done_blocks"`
	TotalSize   int       `db:"total_size"`
	TotalBlocks int       `db:"total_blocks"`
	Nodes       int       `db:"nodes"`
	ExpiredTime time.Time `db:"expired_time"`
	CreateTime  time.Time `db:"created_time"`
	RootCache   bool      `db:"root_cache"`
	EndTime     time.Time `db:"end_time"`
}

// BlockInfo Data Block info
type BlockInfo struct {
	ID          string
	CacheID     string    `db:"cache_id"`
	CID         string    `db:"cid"`
	CIDHash     string    `db:"cid_hash"`
	DeviceID    string    `db:"device_id"`
	Status      int       `db:"status"`
	Size        int       `db:"size"`
	Reliability int       `db:"reliability"`
	CarfileHash string    `db:"carfile_hash"`
	Source      string    `db:"source"`
	FID         int       `db:"fid"`
	CreateTime  time.Time `db:"created_time"`
	EndTime     time.Time `db:"end_time"`
	IsUpdate    bool
}

// MessageInfo Message Info
type MessageInfo struct {
	ID         string
	CID        string    `db:"cid"`
	Target     string    `db:"target"`
	CacheID    string    `db:"cache_id"`
	CarfileCid string    `db:"carfile_cid"`
	Status     MsgStatus `db:"status"`
	Size       int       `db:"size"`
	Type       MsgType   `db:"msg_type"`
	Source     string    `db:"source"`
	CreateTime time.Time `db:"created_time"`
	EndTime    time.Time `db:"end_time"`
}

// // BlockDelete block to be delete
// type BlockDelete struct {
// 	CID      string `db:"cid"`
// 	DeviceID string `db:"device_id"`
// 	Msg      string `db:"msg"`
// 	CacheID  string `db:"cache_id"`
// }

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

// MsgType message type
type MsgType int

const (
	// MsgTypeUnknown type
	MsgTypeUnknown MsgType = iota
	// MsgTypeCache type
	MsgTypeCache
	// MsgTypeDowload type
	MsgTypeDowload
	// MsgTypeValidate type
	MsgTypeValidate
)

// CacheStatus Cache Status
type CacheStatus int

const (
	// CacheStatusUnknown status
	CacheStatusUnknown CacheStatus = iota
	// CacheStatusCreate status
	CacheStatusCreate
	// CacheStatusFail status
	CacheStatusFail
	// CacheStatusSuccess status
	CacheStatusSuccess
	// CacheStatusTimeout status
	CacheStatusTimeout
	// CacheStatusRemove status
	CacheStatusRemove
)

// MsgStatus message Status
type MsgStatus int

const (
	// MsgStatusUnknown status
	MsgStatusUnknown MsgStatus = iota
	// MsgStatusFail status
	MsgStatustusFail
	// MsgStatusSuccess status
	MsgStatusSuccess
)
