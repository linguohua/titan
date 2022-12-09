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
	GetOfflineNodes() ([]*NodeInfo, error)
	SetNodeExited(deviceID string) error

	// Validate Result
	InsertValidateResultInfo(info *ValidateResult) error
	UpdateValidateResultInfo(info *ValidateResult) error
	SetNodeToValidateErrorList(sID, deviceID string) error

	CreateCache(cInfo *api.CacheInfo) error
	SaveCacheEndResults(dInfo *api.DataInfo, cInfo *api.CacheInfo) error
	SaveCacheingResults(dInfo *api.DataInfo, cInfo *api.CacheInfo, updateBlock *api.BlockInfo, createBlocks []*api.BlockInfo) error

	// data info
	SetDataInfo(info *api.DataInfo) error
	GetDataInfo(hash string) (*api.DataInfo, error)
	GetDataCidWithPage(page int) (count int, totalPage int, list []api.DataInfo, err error)
	GetCachesWithData(hash string) ([]string, error)

	ChangeExpiredTimeWhitCaches(carfileHash, cacheID string, expiredTime time.Time) error
	GetExpiredCaches() ([]*api.CacheInfo, error)
	GetMinExpiredTimeWithCaches() (time.Time, error)

	// cache info
	GetCacheInfo(cacheID string) (*api.CacheInfo, error)
	RemoveCacheInfo(cacheID, carfileHash string, isDeleteData bool, reliability int) error
	GetCachesSize(cacheID string, status api.CacheStatus) (int, error)

	// block info
	// SetBlockInfos(infos []*BlockInfo, carfileCid string) error

	GetBlockInfo(cacheID, hash string) (*api.BlockInfo, error)
	GetBlockCountWithStatus(cacheID string, status api.CacheStatus) (int, error)
	GetBlocksWithStatus(cacheID string, status api.CacheStatus) ([]api.BlockInfo, error)
	GetBlocksWithHash(hash string) (map[string]*api.BlockInfo, error)

	GetUndoneBlocks(cacheID string) (map[string]string, error)
	GetAllBlocks(cacheID string) ([]*api.BlockInfo, error)
	GetNodesFromCache(cacheID string) (int, error)
	GetNodesFromData(hash string) (int, error)
	GetCachesFromNode(deviceID string) ([]*api.CacheInfo, error)
	CleanCacheDataWithNode(deviceID string, caches []*api.CacheInfo) error // TODO rename
	// GetNodesFromAllData() ([]string, error)

	// node block
	// DeleteBlockInfos(cacheID, deviceID string, cids []string, removeBlocks int) error
	GetBlocksFID(deviceID string) (map[int]string, error)
	GetBlocksInRange(startFid, endFid int, deviceID string) (map[int]string, error)
	GetBlocksBiggerThan(startFid int, deviceID string) (map[int]string, error)
	CountCidOfDevice(deviceID string) (int64, error)
	GetNodesWithCache(hash string, isSuccess bool) ([]string, error)
	// GetNodeBlock(deviceID, cid string) ([]*BlockInfo, error)

	// temporary node register
	BindRegisterInfo(secret, deviceID string, nodeType api.NodeType) error
	GetRegisterInfo(deviceID string) (*api.NodeRegisterInfo, error)

	// AddDownloadInfo user download block information
	SetBlockDownloadInfo(info *api.BlockDownloadInfo) error
	GetBlockDownloadInfoByDeviceID(deviceID string) ([]*api.BlockDownloadInfo, error)
	GetBlockDownloadInfoByID(id string) (*api.BlockDownloadInfo, error)

	SetEventInfo(info *api.EventInfo) error
	GetEventInfos(page int) (count int, totalPage int, out []api.EventInfo, err error)
	// AddToBeDeleteBlock(infos []*BlockDelete) error
	// RemoveToBeDeleteBlock(infos []*BlockDelete) error
	// GetToBeDeleteBlocks(deviceID string) ([]*BlockDelete, error)
	// SetMessageInfo(infos []*MessageInfo) error

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
	LastTime   time.Time `db:"last_time"`
	Geo        string    `db:"geo"`
	IsOnline   bool      `db:"is_online"`
	NodeType   string    `db:"node_type"`
	Address    string    `db:"address"`
	ServerName string    `db:"server_name"`
	CreateTime time.Time `db:"create_time"`
	URL        string    `db:"url"`
	PrivateKey string    `db:"private_key"`
	Exited     bool      `db:"exited"`
}

// ValidateResult validate result
type ValidateResult struct {
	ID          int
	RoundID     string    `db:"round_id"`
	DeviceID    string    `db:"device_id"`
	ValidatorID string    `db:"validator_id"`
	ServerName  string    `db:"server_name"`
	Msg         string    `db:"msg"`
	Status      int       `db:"status"`
	StartTime   time.Time `db:"start_time"`
	EndTime     time.Time `db:"end_time"`
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

func (v ValidateStatus) Int() int {
	return int(v)
}

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

// MsgStatus message Status
type MsgStatus int

const (
	// MsgStatusUnknown status
	MsgStatusUnknown MsgStatus = iota
	// MsgStatustusFail status
	MsgStatustusFail
	// MsgStatusSuccess status
	MsgStatusSuccess
)
