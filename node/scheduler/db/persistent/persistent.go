package persistent

import (
	"math/rand"
	"time"

	"github.com/linguohua/titan/api"
	"golang.org/x/xerrors"
)

const (
	// NodeTypeKey node info key
	NodeTypeKey = "node_type"
	// SecretKey node info key
	SecretKey = "secret"
)

var myRand = rand.New(rand.NewSource(time.Now().UnixNano()))

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
	UpdateFailValidateResultInfo(info *ValidateResult) error
	SetTimeoutToValidateInfos(info *ValidateResult, deivceIDs []string) error
	UpdateSuccessValidateResultInfo(info *ValidateResult) error
	SummaryValidateMessage(startTime, endTime time.Time, pageNumber, pageSize int) (*api.SummeryValidateResult, error)
	GetRandCarfileWithNode(deviceID string) (string, error)
	AddValidateResultInfos(infos []*ValidateResult) error

	// cache data info
	CreateCarfileReplicaInfo(info *api.CarfileReplicaInfo) error
	UpdateCarfileReplicaInfo(info *api.CarfileReplicaInfo) error
	UpdateCarfileReplicaStatus(hash string, deviceIDs []string, status api.CacheStatus) error

	// data info
	UpdateCarfileRecordCachesInfo(info *api.CarfileRecordInfo) error
	CreateOrUpdateCarfileRecordInfo(info *api.CarfileRecordInfo, isUpdate bool) error
	GetCarfileInfo(hash string) (*api.CarfileRecordInfo, error)
	GetUndoneCarfiles(page int) (*api.DataListInfo, error)
	CarfileRecordExisted(hash string) (bool, error)
	GetCarfileCidWithPage(page int) (info *api.DataListInfo, err error)
	GetCarfileReplicaInfosWithHash(hash string, isSuccess bool) ([]*api.CarfileReplicaInfo, error)
	GetCachesWithCandidate(hash string) ([]string, error)
	ExtendExpiredTimeWhitCarfile(carfileHash string, hour int) error
	ChangeExpiredTimeWhitCarfile(carfileHash string, expiredTime time.Time) error
	GetExpiredCarfiles() ([]*api.CarfileRecordInfo, error)
	GetMinExpiredTime() (time.Time, error)

	// cache info
	GetSucceededCachesCount() (int, error)
	GetCacheInfo(id string) (*api.CarfileReplicaInfo, error)
	GetCacheInfosWithNode(deviceID string, index, count int) (*api.NodeCacheRsp, error)
	// ResetCarfileRecordInfo(info *api.CacheCarfileInfo) error
	RemoveCarfileReplica(deviceID, carfileHash string) error
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

	SetCacheEventInfo(info *api.CacheEventInfo) error
	ListCacheEventInfos(page int, cid string) (info *api.EventListInfo, err error)

	SetNodeUpdateInfo(info *api.NodeAppUpdateInfo) error
	GetNodeUpdateInfos() (map[int]*api.NodeAppUpdateInfo, error)

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
