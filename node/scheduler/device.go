package scheduler

import (
	"fmt"
	"time"

	"titan/api"
	"titan/node/scheduler/redishelper"

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

const (
	// redis field
	lastTimeField   = "LastTime"
	onLineTimeField = "OnLineTime"
	isOnLineField   = "IsOnLine"
)

// DeviceOnline Save DeciceInfo
func DeviceOnline(deviceID string, onlineTime int) error {
	lastTime := time.Now().Format("2006-01-02 15:04:05")

	key := fmt.Sprintf(redishelper.RedisKeyDeviceInfo, deviceID)
	err := redishelper.RedisHMSET(key, lastTimeField, lastTime, isOnLineField, true)
	if err != nil {
		return err
	}

	return redishelper.RedisHINCRBY(key, onLineTimeField, onlineTime)
}

// DeviceOffline offline
func DeviceOffline(deviceID string) error {
	key := fmt.Sprintf(redishelper.RedisKeyDeviceInfo, deviceID)
	return redishelper.RedisHMSET(key, isOnLineField, false)
}

// LoadDeciceInfo Load DeciceInfo
func LoadDeciceInfo(deviceID string) error {
	return nil
}
