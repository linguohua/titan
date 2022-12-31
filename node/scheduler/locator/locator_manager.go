package locator

import (
	"context"

	"github.com/linguohua/titan/node/scheduler/area"
	"github.com/linguohua/titan/node/scheduler/node"
)

// Manager Locator Manager
type Manager struct {
	locatorMap map[string]*node.Locator
	port       int
}

// NewLoactorManager new
func NewLoactorManager(port int) *Manager {
	return &Manager{
		locatorMap: make(map[string]*node.Locator),
		port:       port,
	}
}

// AddLocator add
func (m *Manager) AddLocator(location *node.Locator) {
	m.locatorMap[location.GetLocatorID()] = location
}

// NotifyNodeStatusToLocator Notify Node Status To Locator
func (m *Manager) NotifyNodeStatusToLocator(deviceID string, isOnline bool) {
	// log.Warnf("notifyNodeStatusToLocator : %v", m.locatorMap)
	for _, locator := range m.locatorMap {
		// log.Warnf("locator : %v", locator)
		if locator != nil && locator.GetAPI() != nil {
			if isOnline {
				locator.GetAPI().DeviceOnline(context.Background(), deviceID, area.GetServerArea(), m.port)
			} else {
				locator.GetAPI().DeviceOffline(context.Background(), deviceID)
			}
		}
	}
}
