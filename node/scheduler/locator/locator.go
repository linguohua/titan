package locator

import (
	"context"
	"sync"
	"time"

	"github.com/filecoin-project/go-jsonrpc"
	logging "github.com/ipfs/go-log/v2"
	"github.com/linguohua/titan/api"
)

// TODO node status can be recorded to the etcd, so there is no need for a locator here

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

func New(api api.Locator, closer jsonrpc.ClientCloser, locatorID string) *Locator {
	locator := &Locator{
		nodeAPI:   api,
		closer:    closer,
		locatorID: locatorID,
	}

	return locator
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
func StoreLocator(locator *Locator) {
	locators.Store(locator.GetLocatorID(), locator)
}

// ChangeNodeOnlineStatus Notify Node Status To Locator
func ChangeNodeOnlineStatus(nodeID string, isOnline bool) {
	locators.Range(func(key, value interface{}) bool {
		locator := value.(*Locator)

		if locator != nil && locator.GetAPI() != nil {
			go func() {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()

				err := locator.GetAPI().SetNodeOnlineStatus(ctx, nodeID, isOnline)
				if err != nil {
					log.Errorf("SetNodeOnlineStatus error:%s", err.Error())
				}
			}()
		}
		return true
	})
}
