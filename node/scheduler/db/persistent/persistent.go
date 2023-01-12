package persistent

import (
	"time"

	"github.com/linguohua/titan/api"
	"golang.org/x/xerrors"
)

const (
	//NodeTypeKey node info key
	NodeTypeKey = "node_type"
	//SecretKey node info key
	SecretKey = "secret"
)

// DB Persistent db
type DB interface {
	IsNilErr(err error) bool

	// Node Info
	SetNodeInfo(deviceID string, info *NodeInfo) error
	SetNodeOffline(deviceID string, lastTime time.Time) error
	GetNodeAuthInfo(deviceID string) (*api.DownloadServerAccessAuth, error)
	GetOfflineNodes() ([]*NodeInfo, error)
	SetNodesQuit(deviceIDs []string) error

	// Validate Result
	InsertValidateResultInfo(info *ValidateResult) error
	UpdateFailValidateResultInfo(info *ValidateResult) error
	UpdateSuccessValidateResultInfo(info *ValidateResult) error
	SummaryValidateMessage(startTime, endTime time.Time, pageNumber, pageSize int) (*api.SummeryValidateResult, error)

	// cache data info
	CreateCache(cInfo *api.CacheTaskInfo) error
	SaveCacheResults(dInfo *api.CarfileRecordInfo, cInfo *api.CacheTaskInfo) error
	UpdateCacheInfoOfTimeoutNodes(deviceIDs []string) error

	// data info
	SetDataInfo(info *api.CarfileRecordInfo) error
	GetDataInfo(hash string) (*api.CarfileRecordInfo, error)
	GetDataCidWithPage(page int) (count int, totalPage int, list []*api.CarfileRecordInfo, err error)
	GetCachesWithData(hash string, isSuccess bool) ([]*api.CacheTaskInfo, error)
	ExtendExpiredTimeWhitCaches(carfileHash, deviceID string, hour int) error
	ChangeExpiredTimeWhitCaches(carfileHash, deviceID string, expiredTime time.Time) error
	GetExpiredCaches() ([]*api.CacheTaskInfo, error)
	GetMinExpiredTimeWithCaches() (time.Time, error)

	// cache info
	GetSuccessCaches() ([]*api.CacheTaskInfo, error)
	GetCacheInfo(carfileHash, deviceID string) (*api.CacheTaskInfo, error)
	RemoveCacheAndUpdateData(deviceID, carfileHash string, reliability int) error
	RemoveCarfileRecord(carfileHash string) error
	UpdateCacheInfoOfQuitNode(deviceIDs []string) ([]*api.CarfileRecordInfo, error)

	// temporary node register
	BindRegisterInfo(secret, deviceID string, nodeType api.NodeType) error
	GetRegisterInfo(deviceID, key string, out interface{}) error

	// download info
	SetBlockDownloadInfo(info *api.BlockDownloadInfo) error
	GetBlockDownloadInfoByDeviceID(deviceID string) ([]*api.BlockDownloadInfo, error)
	GetBlockDownloadInfoByID(id string) (*api.BlockDownloadInfo, error)
	GetNodesByUserDownloadBlockIn(minute int) ([]string, error)

	// cache event info
	SetEventInfo(info *api.EventInfo) error
	GetEventInfos(page int) (count int, totalPage int, out []*api.EventInfo, err error)

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
	Quitted    bool      `db:"quitted"`
}

// ValidateResult validate result
type ValidateResult struct {
	ID          int
	RoundID     int64     `db:"round_id"`
	DeviceID    string    `db:"device_id"`
	ValidatorID string    `db:"validator_id"`
	ServerName  string    `db:"server_name"`
	BlockNumber int64     `db:"block_number"` // number of blocks verified
	Status      int       `db:"status"`
	Duration    int64     `db:"duration"` // validate duration, microsecond
	Bandwidth   float64   `db:"bandwidth"`
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
	// ValidateStatusSuccess status
	ValidateStatusSuccess
	// ValidateStatusTimeOut status
	ValidateStatusTimeOut
	// ValidateStatusCancel status
	ValidateStatusCancel
	// ValidateStatusFail status
	ValidateStatusFail
	// ValidateStatusOther status
	ValidateStatusOther
)

// Int to int
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
