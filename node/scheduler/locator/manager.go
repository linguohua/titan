package locator

import (
	"context"
	"sync"

	logging "github.com/ipfs/go-log/v2"
	"github.com/linguohua/titan/node/scheduler/node"
)

var log = logging.Logger("locator")

// Manager Locator Manager
type Manager struct {
	locators sync.Map
	port     int
}

// NewManager new
func NewManager() *Manager {
	return &Manager{}
}

// AddLocator add
func (m *Manager) AddLocator(location *node.Locator) {
	m.locators.Store(location.GetLocatorID(), location)
}

// NotifyNodeStatusToLocator Notify Node Status To Locator
func (m *Manager) NotifyNodeStatusToLocator(deviceID string, isOnline bool) {
	m.locators.Range(func(key, value interface{}) bool {
		locator := value.(*node.Locator)

		if locator != nil && locator.GetAPI() != nil {
			go func() {
				err := locator.GetAPI().SetDeviceOnlineStatus(context.Background(), deviceID, isOnline)
				if err != nil {
					log.Errorf("NotifyNodeStatusToLocator error:%s", err.Error())
				}
			}()
		}
		return true
	})
}
