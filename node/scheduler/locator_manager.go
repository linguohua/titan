package scheduler

import (
	"context"

	sCommon "github.com/linguohua/titan/node/scheduler/common"
	"github.com/linguohua/titan/node/scheduler/node"
)

// LocatorManager Locator Manager
type LocatorManager struct {
	locatorMap map[string]*node.Location
	port       int
}

func newLoactorManager(port int) *LocatorManager {
	return &LocatorManager{
		locatorMap: make(map[string]*node.Location),
		port:       port,
	}
}

func (m *LocatorManager) addLocator(location *node.Location) {
	m.locatorMap[location.LocatorID] = location
}

func (m *LocatorManager) notifyNodeStatusToLocator(deviceID string, isOnline bool) {
	// log.Warnf("notifyNodeStatusToLocator : %v", m.locatorMap)
	for _, locator := range m.locatorMap {
		// log.Warnf("locator : %v", locator)
		if locator != nil && locator.NodeAPI != nil {
			if isOnline {
				locator.NodeAPI.DeviceOnline(context.Background(), deviceID, sCommon.ServerArea, m.port)
			} else {
				locator.NodeAPI.DeviceOffline(context.Background(), deviceID)
			}
		}
	}
}
