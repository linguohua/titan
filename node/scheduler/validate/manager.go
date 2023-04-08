package validate

import (
	"context"
	"sync"

	"github.com/docker/go-units"
	"github.com/filecoin-project/pubsub"
	logging "github.com/ipfs/go-log/v2"
	"github.com/linguohua/titan/api/types"
	"github.com/linguohua/titan/node/modules/dtypes"
	"github.com/linguohua/titan/node/scheduler/node"
)

var log = logging.Logger("node")

const (
	bandwidthRatio = 0.7                    // The ratio of the total upstream bandwidth on edge nodes to the downstream bandwidth on validation nodes.
	toleranceBwUp  = float64(5 * units.MiB) // The tolerance for uplink bandwidth deviation per group, set to 5M.
)

// VWindow represents a validation window that contains a validator id and BeValidates list.
type VWindow struct {
	NodeID      string // Node ID of the validation window.
	BeValidates map[string]float64
}

func newVWindow(nID string) *VWindow {
	return &VWindow{
		NodeID:      nID,
		BeValidates: make(map[string]float64),
	}
}

// BeValidateGroup Each BeValidateGroup will be paired with a VWindow
type BeValidateGroup struct {
	sumBwUp     float64
	beValidates map[string]float64
	lock        sync.RWMutex
}

func newBeValidateGroup() *BeValidateGroup {
	return &BeValidateGroup{
		beValidates: make(map[string]float64),
	}
}

// Manager validate manager
type Manager struct {
	nodeMgr *node.Manager
	notify  *pubsub.PubSub

	// Each validator provides n window(VWindow) for titan according to the bandwidth down, and each window corresponds to a group(BeValidateGroup).
	// All nodes will randomly fall into a group(BeValidateGroup).
	// When the validate starts, the window is paired with the group.
	validatePairLock sync.RWMutex
	vWindows         []*VWindow         // The validator allocates n window according to the size of the bandwidth down
	beValidateGroups []*BeValidateGroup // Each VWindow has a BeValidateGroup
	unpairedGroup    *BeValidateGroup   // Save unpaired BeValidates

	seed       int64
	curRoundID string
	close      chan struct{}
	config     dtypes.GetSchedulerConfigFunc

	updateCh chan struct{}
}

// NewManager return new node manager instance
func NewManager(nodeMgr *node.Manager, configFunc dtypes.GetSchedulerConfigFunc, p *pubsub.PubSub) *Manager {
	nodeManager := &Manager{
		nodeMgr:       nodeMgr,
		config:        configFunc,
		close:         make(chan struct{}),
		unpairedGroup: newBeValidateGroup(),
		updateCh:      make(chan struct{}, 1),
		notify:        p,
	}

	return nodeManager
}

// Start start validate and elect task
func (m *Manager) Start(ctx context.Context) {
	go m.startValidation(ctx)
	go m.electTicker()

	m.subscribe()
}

// Stop stop
func (m *Manager) Stop(ctx context.Context) error {
	return m.stopValidate(ctx)
}

func (m *Manager) subscribe() {
	subOnline := m.notify.Sub(types.EventNodeOnline.String())
	subOffline := m.notify.Sub(types.EventNodeOffline.String())

	go func() {
		defer m.notify.Unsub(subOnline)
		defer m.notify.Unsub(subOffline)

		for {
			select {
			case u := <-subOnline:
				node := u.(*node.Node)
				m.nodeStateChange(node, true)
			case u := <-subOffline:
				node := u.(*node.Node)
				m.nodeStateChange(node, false)
			}
		}
	}()
}

// nodeStateChange  changes in the state of a node (i.e. whether it comes online or goes offline)
func (m *Manager) nodeStateChange(node *node.Node, isOnline bool) {
	if node == nil {
		return
	}

	nodeID := node.NodeInfo.NodeID

	isV, err := m.nodeMgr.IsValidator(nodeID)
	if err != nil {
		log.Errorf("IsValidator err:%s", err.Error())
		return
	}

	if isOnline {
		if isV {
			m.addValidator(nodeID, node.DownloadSpeed)
		} else {
			m.addBeValidate(nodeID, node.DownloadSpeed)
		}

		return
	}

	if isV {
		m.removeValidator(nodeID)
	} else {
		m.removeBeValidate(nodeID)
	}
}
