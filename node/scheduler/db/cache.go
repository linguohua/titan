package db

import (
	"fmt"

	"github.com/linguohua/titan/api"
)

// CacheDB cache db
type CacheDB interface {
	IncrNodeCacheTag(deviceID string) (int64, error)
	IncrSpotCheckID() (int64, error)
	GetSpotCheckID() (string, error)

	DelNodeWithSpotCheckList(deviceID string) error
	SetNodeToSpotCheckList(deviceID string) error
	GetNodesWithSpotCheckList() ([]string, error)
	DelSpotCheckList() error

	DelCacheDataInfo(deviceID, cid string) error
	SetCacheDataInfo(deviceID, cid string, tag int64) error
	GetCacheDataInfo(deviceID, cid string) (string, error)
	GetCacheDataInfos(deviceID string) (map[string]string, error)

	DelNodeWithCacheList(deviceID, cid string) error
	SetNodeToCacheList(deviceID, cid string) error
	GetNodesWithCacheList(cid string) ([]string, error)
	IsNodeInCacheList(cid, deviceID string) (bool, error)

	SetNodeInfo(deviceID string, info NodeInfo) error
	GetNodeInfo(deviceID string) (NodeInfo, error)

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

	SetSpotCheckResultInfo(sID string, edgeID, validator, msg string, status SpotCheckStatus) error
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

// SpotCheckStatus Spot Check Status
type SpotCheckStatus int

const (
	// SpotCheckStatusUnknown status
	SpotCheckStatusUnknown SpotCheckStatus = iota
	// SpotCheckStatusCreate status
	SpotCheckStatusCreate
	// SpotCheckStatusTimeOut status
	SpotCheckStatusTimeOut
	// SpotCheckStatusSuccess status
	SpotCheckStatusSuccess
	// SpotCheckStatusFail status
	SpotCheckStatusFail
)
