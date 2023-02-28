package locator

import (
	"context"
	"sync"

	"github.com/filecoin-project/go-jsonrpc"
	logging "github.com/ipfs/go-log/v2"
	"github.com/linguohua/titan/api"
)

var (
	log      = logging.Logger("locator")
	locators sync.Map
)

// Locator Edge node
type Locator struct {
	nodeAPI api.Locator
	closer  jsonrpc.ClientCloser

	locatorID string
}

// New new location
func New(api api.Locator, closer jsonrpc.ClientCloser, locatorID string) *Locator {
	location := &Locator{
		nodeAPI:   api,
		closer:    closer,
		locatorID: locatorID,
	}

	return location
}

// GetAPI get node api
func (l *Locator) GetAPI() api.Locator {
	return l.nodeAPI
}

// GetLocatorID get id
func (l *Locator) GetLocatorID() string {
	return l.locatorID
}

// StoreLocator add
func StoreLocator(location *Locator) {
	locators.Store(location.GetLocatorID(), location)
}

// ChangeNodeOnlineStatus Notify Node Status To Locator
func ChangeNodeOnlineStatus(deviceID string, isOnline bool) {
	locators.Range(func(key, value interface{}) bool {
		locator := value.(*Locator)

		if locator != nil && locator.GetAPI() != nil {
			go func() {
				err := locator.GetAPI().SetDeviceOnlineStatus(context.Background(), deviceID, isOnline)
				if err != nil {
					log.Errorf("SetDeviceOnlineStatus error:%s", err.Error())
				}
			}()
		}
		return true
	})
}
