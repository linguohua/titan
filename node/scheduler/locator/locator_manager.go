package locator

import (
	"context"
	"sync"

	"github.com/linguohua/titan/node/scheduler/node"
	"github.com/linguohua/titan/region"
)

// Manager Locator Manager
type Manager struct {
	locatorMap sync.Map
	port       int
}

// NewLoactorManager new
func NewLoactorManager(port int) *Manager {
	return &Manager{
		port: port,
	}
}

// AddLocator add
func (m *Manager) AddLocator(location *node.Locator) {
	m.locatorMap.Store(location.GetLocatorID(), location)
}

// NotifyNodeStatusToLocator Notify Node Status To Locator
func (m *Manager) NotifyNodeStatusToLocator(deviceID string, isOnline bool) {
	area := region.DefaultGeoInfo("").Geo

	m.locatorMap.Range(func(key, value interface{}) bool {
		locator := value.(*node.Locator)

		if locator != nil && locator.GetAPI() != nil {
			if isOnline {
				go locator.GetAPI().DeviceOnline(context.Background(), deviceID, area, m.port)
			} else {
				go locator.GetAPI().DeviceOffline(context.Background(), deviceID)
			}
		}
		return true
	})
}
