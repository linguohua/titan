package scheduler

import "context"

// LocatorManager Locator Manager
type LocatorManager struct {
	locatorMap map[string]*Location
	port       int
}

func newLoactorManager(port int) *LocatorManager {
	return &LocatorManager{
		locatorMap: make(map[string]*Location),
		port:       port,
	}
}

func (m *LocatorManager) addLocator(location *Location) {
	m.locatorMap[location.locatorID] = location
}

func (m *LocatorManager) notifyNodeStatusToLocator(deviceID string, isOnline bool) {
	// log.Warnf("notifyNodeStatusToLocator : %v", m.locatorMap)
	for _, locator := range m.locatorMap {
		// log.Warnf("locator : %v", locator)
		if locator != nil && locator.nodeAPI != nil {
			if isOnline {
				locator.nodeAPI.DeviceOnline(context.Background(), deviceID, serverArea, m.port)
			} else {
				locator.nodeAPI.DeviceOffline(context.Background(), deviceID)
			}
		}
	}
}
