package db

import (
	"fmt"

	"github.com/linguohua/titan/api"
)

// CacheDB cache db
type CacheDB interface {
	IncrNodeCacheTag(deviceID string) (int64, error)
	GetNodeCacheTag(deviceID string) (int64, error)
	IncrVerifyID() (int64, error)
	GetVerifyID() (string, error)

	DelNodeWithVerifyList(deviceID string) error
	SetNodeToVerifyList(deviceID string) error
	GetNodesWithVerifyList() ([]string, error)
	DelVerifyList() error

	DelCacheDataInfo(deviceID, cid string) error
	SetCacheDataInfo(deviceID, cid string, tag string) error
	GetCacheDataInfo(deviceID, cid string) (string, error)
	GetCacheDataInfos(deviceID string) (map[string]string, error)

	DelCacheDataTagInfo(deviceID, tag string) error
	SetCacheDataTagInfo(deviceID, cid string, tag string) error
	GetCacheDataTagInfo(deviceID, tag string) (string, error)
	GetCacheDataTagInfos(deviceID string) (map[string]string, error)

	DelNodeWithCacheList(deviceID, cid string) error
	SetNodeToCacheList(deviceID, cid string) error
	GetNodesWithCacheList(cid string) ([]string, error)
	IsNodeInCacheList(cid, deviceID string) (bool, error)

	SetNodeInfo(deviceID string, info NodeInfo) error
	GetNodeInfo(deviceID string) (NodeInfo, error)
	AddNodeOnlineTime(deviceID string, onlineTime int64) error

	DelNodeWithGeoList(deviceID, geo string) error
	SetNodeToGeoList(deviceID, geo string) error
	GetNodesWithGeoList(geo string) ([]string, error)

	// SetNodeToNodeList(deviceID string, typeName api.NodeTypeName) error
	// GetNodesWithNodeList(typeName api.NodeTypeName) ([]string, error)
	// DelNodeWithNodeList(deviceID string, typeName api.NodeTypeName) error

	SetGeoToList(geo string) error
	GetGeosWithList() ([]string, error)
	DelGeoWithList(geo string) error

	SetValidatorToList(deviceID string) error
	GetValidatorsWithList() ([]string, error)
	DelValidatorList() error
	IsNodeInValidatorList(deviceID string) (bool, error)

	SetGeoToValidatorList(deviceID, geo string) error
	GetGeoWithValidatorList(deviceID string) ([]string, error)
	DelValidatorGeoList(deviceID string) error

	SetVerifyResultInfo(sID string, edgeID, validator, msg string, status VerifyStatus) error
	SetNodeToVerifyOfflineList(sID, deviceID string) error

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
func NewCacheDB(url string, dbType string) {
	var err error

	switch dbType {
	case TypeRedis():
		cacheDB, err = InitRedis(url)
	default:
		panic("unknown CacheDB type")
	}

	if err != nil {
		e := fmt.Sprintf("NewCacheDB err:%v , url:%v", err, url)
		panic(e)
	}
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

// VerifyStatus Verify Status
type VerifyStatus int

const (
	// VerifyStatusUnknown status
	VerifyStatusUnknown VerifyStatus = iota
	// VerifyStatusCreate status
	VerifyStatusCreate
	// VerifyStatusTimeOut status
	VerifyStatusTimeOut
	// VerifyStatusSuccess status
	VerifyStatusSuccess
	// VerifyStatusFail status
	VerifyStatusFail
)
