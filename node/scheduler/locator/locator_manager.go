package locator

import (
	"context"

	"github.com/linguohua/titan/node/scheduler/area"
	"github.com/linguohua/titan/node/scheduler/node"
)

// Manager Locator Manager
type Manager struct {
	locatorMap map[string]*node.Location
	port       int
}

// NewLoactorManager new
func NewLoactorManager(port int) *Manager {
	return &Manager{
		locatorMap: make(map[string]*node.Location),
		port:       port,
	}
}

// AddLocator add
func (m *Manager) AddLocator(location *node.Location) {
	m.locatorMap[location.LocatorID] = location
}

// NotifyNodeStatusToLocator Notify Node Status To Locator
func (m *Manager) NotifyNodeStatusToLocator(deviceID string, isOnline bool) {
	// log.Warnf("notifyNodeStatusToLocator : %v", m.locatorMap)
	for _, locator := range m.locatorMap {
		// log.Warnf("locator : %v", locator)
		if locator != nil && locator.NodeAPI != nil {
			if isOnline {
				locator.NodeAPI.DeviceOnline(context.Background(), deviceID, area.GetServerArea(), m.port)
			} else {
				locator.NodeAPI.DeviceOffline(context.Background(), deviceID)
			}
		}
	}
}
