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
	GetDataInfo(cid string) (*DataInfo, error)
	GetDataCidWithPage(page int) (count int, totalPage int, list []string, err error)
	GetCachesWithData(cid string) ([]string, error)

	// cache info
	GetCacheInfo(cacheID string) (*CacheInfo, error)
	RemoveAndUpdateCacheInfo(cacheID, carfileID, rootCacheID string, isDeleteData bool, reliability int) error
	GetCachesSize(cacheID string, status int) (int, error)

	// block info
	SetBlockInfos(infos []*BlockInfo, carfileCid string) error
	GetBlockInfo(cacheID, cid, deviceID string) (*BlockInfo, error)
	GetBloackCountWithStatus(cacheID string, status int) (int, error)
	GetUndoneBlocks(cacheID string) (map[string]string, error)
	GetAllBlocks(cacheID string) (map[string][]string, error)
	GetNodesFromCache(cacheID string) (int, error)
	GetNodesFromData(cid string) (int, error)

	// node block
	// DeleteBlockInfos(cacheID, deviceID string, cids []string, removeBlocks int) error
	GetBlocksFID(deviceID string) (map[int]string, error)
	GetBlocksInRange(deviceID string, startFid, endFid int) (map[int]string, error)
	GetDeviceBlockNum(deviceID string) (int64, error)
	GetNodesWithCache(cid string, isSuccess bool) ([]string, error)
	// GetNodeBlock(deviceID, cid string) ([]*BlockInfo, error)

	// temporary node register
	BindRegisterInfo(secret, deviceID string, nodeType api.NodeType) error
	GetRegisterInfo(deviceID string) (*api.NodeRegisterInfo, error)

	// AddDownloadInfo user download block information
	AddDownloadInfo(deviceID string, info *api.BlockDownloadInfo) error
	GetDownloadInfo(deviceID string) ([]*api.BlockDownloadInfo, error)

	SetEventInfo(info *EventInfo) error
	// AddToBeDeleteBlock(infos []*BlockDelete) error
	// RemoveToBeDeleteBlock(infos []*BlockDelete) error
	// GetToBeDeleteBlocks(deviceID string) ([]*BlockDelete, error)

	// tool
	ReplaceArea() string
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
	ID          int
	DeviceID    string    `db:"device_id"`
	LastTime    string    `db:"last_time"`
	Geo         string    `db:"geo"`
	IsOnline    int       `db:"is_online"`
	NodeType    string    `db:"node_type"`
	Address     string    `db:"address"`
	ServerName  string    `db:"server_name"`
	CreateTime  time.Time `db:"create_time"`
	URL         string    `db:"url"`
	SecurityKey string    `db:"security"`
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

// // NodeBlock Node Block
// type NodeBlock struct {
// 	DeviceID  string `db:"device_id"`
// 	FID       string `db:"fid"`
// 	CID       string `db:"cid"`
// 	CarfileID string `db:"carfile_id"`
// 	CacheID   string `db:"cache_id"`
// }

// // BlockNodes Node Block
// type BlockNodes struct {
// 	ID       int
// 	DeviceID string `db:"device_id"`
// 	CID      string `db:"cid"`
// }

// DataInfo Data info
type DataInfo struct {
	CID             string    `db:"cid"`
	Status          int       `db:"status"`
	Reliability     int       `db:"reliability"`
	NeedReliability int       `db:"need_reliability"`
	CacheCount      int       `db:"cache_count"`
	RootCacheID     string    `db:"root_cache_id"`
	TotalSize       int       `db:"total_size"`
	TotalBlocks     int       `db:"total_blocks"`
	Nodes           int       `db:"nodes"`
	ExpiredTime     time.Time `db:"expired_time"`
	CreateTime      time.Time `db:"created_time"`
}

// CacheInfo Data Block info
type CacheInfo struct {
	CarfileID    string    `db:"carfile_id"`
	CacheID      string    `db:"cache_id"`
	Status       int       `db:"status"`
	Reliability  int       `db:"reliability"`
	DoneSize     int       `db:"done_size"`
	DoneBlocks   int       `db:"done_blocks"`
	TotalSize    int       `db:"total_size"`
	TotalBlocks  int       `db:"total_blocks"`
	RemoveBlocks int       `db:"remove_blocks"`
	Nodes        int       `db:"nodes"`
	ExpiredTime  time.Time `db:"expired_time"`
	CreateTime   time.Time `db:"created_time"`
	RootCache    bool      `db:"root_cache"`
}

// BlockInfo Data Block info
type BlockInfo struct {
	ID          string
	CacheID     string `db:"cache_id"`
	CID         string `db:"cid"`
	DeviceID    string `db:"device_id"`
	Status      int    `db:"status"`
	Size        int    `db:"size"`
	Reliability int    `db:"reliability"`
	CarfileID   string `db:"carfile_id"`
	FID         int    `db:"fid"`
	IsUpdate    bool
}

// EventInfo Event Info
type EventInfo struct {
	ID         int
	CID        string    `db:"cid"`
	DeviceID   string    `db:"device_id"`
	User       string    `db:"user"`
	Event      string    `db:"event"`
	Msg        string    `db:"msg"`
	CreateTime time.Time `db:"created_time"`
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
