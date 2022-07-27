package scheduler

import (
	"fmt"
	"time"

	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/node/scheduler/db"

	"github.com/filecoin-project/go-jsonrpc"
)

// EdgeNode Edge 节点
type EdgeNode struct {
	edgeAPI api.Edge
	closer  jsonrpc.ClientCloser

	deviceID string
	addr     string
	userID   string
}

// CandidateNode Candidate 节点
type CandidateNode struct {
	edgeAPI api.Candidate
	closer  jsonrpc.ClientCloser

	deviceID string
	addr     string
	userID   string
}

const (
	// redis field
	lastTimeField   = "LastTime"
	onLineTimeField = "OnLineTime"
	isOnLineField   = "IsOnLine"
)

// DeviceOnline Save DeciceInfo
func DeviceOnline(deviceID string, onlineTime int64) error {
	lastTime := time.Now().Format("2006-01-02 15:04:05")

	key := fmt.Sprintf(db.RedisKeyDeviceInfo, deviceID)
	err := cacheDB.HSetValues(key, lastTimeField, lastTime, isOnLineField, true)
	if err != nil {
		return err
	}

	return cacheDB.IncrbyField(key, onLineTimeField, onlineTime)
}

// DeviceOffline offline
func DeviceOffline(deviceID string) error {
	key := fmt.Sprintf(db.RedisKeyDeviceInfo, deviceID)
	return cacheDB.HSetValue(key, isOnLineField, false)
}

// LoadDeciceInfo Load DeciceInfo
func LoadDeciceInfo(deviceID string) error {
	return nil
}
