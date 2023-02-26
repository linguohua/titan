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
	GetNodePrivateKey(deviceID string) (string, error)
	GetOfflineNodes() ([]*NodeInfo, error)
	SetNodesQuit(deviceIDs []string) error
	SetNodePort(deviceID, port string) error

	// Validate Result
	SetTimeoutToValidateInfos(roundID int64, deivceIDs []string) error
	UpdateValidateResultInfo(info *api.ValidateResult) error
	SummaryValidateMessage(startTime, endTime time.Time, pageNumber, pageSize int) (*api.SummeryValidateResult, error)
	GetRandCarfileWithNode(deviceID string) (string, error)
	AddValidateResultInfos(infos []*api.ValidateResult) error

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
	ChangeExpiredTimeWhitCarfile(carfileHash string, expiredTime time.Time) error
	GetExpiredCarfiles() ([]*api.CarfileRecordInfo, error)
	GetMinExpiredTime() (time.Time, error)

	// cache info
	GetSucceededCachesCount() (int, error)
	GetReplicaInfo(id string) (*api.CarfileReplicaInfo, error)
	GetCacheInfosWithNode(deviceID string, index, count int) (*api.NodeCacheRsp, error)
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

	SetNodeUpdateInfo(info *api.NodeAppUpdateInfo) error
	GetNodeUpdateInfos() (map[int]*api.NodeAppUpdateInfo, error)
	DeleteNodeUpdateInfo(nodeType int) error

	// web
	webDB
}

var (
	db DB

	serverID string
)

// NewDB New  DB
func NewDB(url, dbType, sID string) error {
	var err error

	serverID = sID

	switch dbType {
	case TypeSQL():
		db, err = InitSQL(url)
	default:
		err = xerrors.New("unknown DB type")
	}

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
	Port       string    `db:"port"`
	ServerID   string    `db:"server_id"`
	CreateTime time.Time `db:"create_time"`
	PrivateKey string    `db:"private_key"`
	Quitted    bool      `db:"quitted"`
}
