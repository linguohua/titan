package db

import (
	"github.com/linguohua/titan/api"
	"golang.org/x/xerrors"
)

// CacheDB cache db
type CacheDB interface {
	IncrValidateRoundID() (int64, error)
	GetValidateRoundID() (string, error)

	RemoveNodeWithValidateingList(deviceID string) error
	SetNodeToValidateingList(deviceID string) error
	GetNodesWithValidateingList() ([]string, error)
	RemoveValidateingList() error

	RemoveCacheBlockInfo(deviceID, cid string) error
	SetCacheBlockInfo(deviceID, cid string) error
	GetCacheBlockInfo(deviceID, cid string) (int64, error)
	GetCacheBlockNum(deviceID string) (int64, error)
	GetCacheBlockInfos(deviceID string, start, end int64) ([]string, error)

	RemoveNodeWithCacheList(deviceID, cid string) error
	SetNodeToCacheList(deviceID, cid string) error
	GetNodesWithCacheList(cid string) ([]string, error)
	IsNodeInCacheList(cid, deviceID string) (bool, error)

	SetNodeInfo(deviceID string, info *NodeInfo) error
	GetNodeInfo(deviceID string) (*NodeInfo, error)
	AddNodeOnlineTime(deviceID string, onlineTime int64) error

	RemoveNodeWithGeoList(deviceID, geo string) error
	SetNodeToGeoList(deviceID, geo string) error
	GetNodesWithGeoList(geo string) ([]string, error)

	// SetNodeToNodeList(deviceID string, typeName api.NodeTypeName) error
	// GetNodesWithNodeList(typeName api.NodeTypeName) ([]string, error)
	// RemoveNodeWithNodeList(deviceID string, typeName api.NodeTypeName) error

	SetGeoToList(geo string) error
	GetGeosWithList() ([]string, error)
	RemoveGeoWithList(geo string) error

	SetValidatorToList(deviceID string) error
	GetValidatorsWithList() ([]string, error)
	RemoveValidatorList() error
	IsNodeInValidatorList(deviceID string) (bool, error)

	SetGeoToValidatorList(deviceID, geo string) error
	GetGeoWithValidatorList(deviceID string) ([]string, error)
	RemoveValidatorGeoList(deviceID string) error

	SetValidateResultInfo(sID string, edgeID, validator, msg string, status ValidateStatus) error
	SetNodeToValidateErrorList(sID, deviceID string) error

	SetEdgeDeviceIDList(deviceIDs []string) error
	IsEdgeInDeviceIDList(deviceID string) (bool, error)
	SetCandidateDeviceIDList(deviceIDs []string) error
	IsCandidateInDeviceIDList(deviceID string) (bool, error)

	IsNilErr(err error) bool
}

var cacheDB CacheDB

// NotFind not find data
const NotFind = "not find"

// NewCacheDB New Cache DB
func NewCacheDB(url string, dbType string) error {
	var err error

	switch dbType {
	case TypeRedis():
		cacheDB, err = InitRedis(url)
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

// GetCacheDB Get CacheDB
func GetCacheDB() CacheDB {
	return cacheDB
}

// NodeInfo base info
type NodeInfo struct {
	OnLineTime int64
	LastTime   string
	Geo        string
	IsOnline   bool
	NodeType   api.NodeTypeName
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
)
