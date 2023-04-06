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
	bandwidthRatio    = 0.7                      // The ratio of the total upstream bandwidth on edge nodes to the downstream bandwidth on validation nodes.
	baseBandwidthDown = float64(100 * units.MiB) // 100M Validator unit bandwidth down TODO save to config
	toleranceBwUp     = float64(5 * units.MiB)   // 5M Tolerance uplink bandwidth deviation per group
)

// ValidatorBwDnUnit represents the bandwidth down unit of the validator
type ValidatorBwDnUnit struct {
	NodeID      string
	BeValidates map[string]float64
}

func newValidatorDwDnUnit(nID string) *ValidatorBwDnUnit {
	return &ValidatorBwDnUnit{
		NodeID:      nID,
		BeValidates: make(map[string]float64),
	}
}

// BeValidateGroup BeValidate Group
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
	// Each validator provides n units(ValidatorBwDnUnit) for titan according to the bandwidth down, and each unit corresponds to a group(BeValidateGroup).
	// All nodes will randomly fall into a group(BeValidateGroup).
	// When the validate starts, the unit is paired with the group.
	validatePairLock sync.RWMutex
	validatorUnits   []*ValidatorBwDnUnit // The validator allocates n units according to the size of the bandwidth down
	beValidateGroups []*BeValidateGroup   // Each ValidatorBwDnUnit has a BeValidateGroup
	unpairedGroup    *BeValidateGroup     // Save unpaired BeValidates

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
	subOnline := m.notify.Sub(types.TopicsNodeOnline.String())
	subOffline := m.notify.Sub(types.TopicsNodeOffline.String())

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
			m.addValidator(nodeID, node.BandwidthDown)
		} else {
			m.addBeValidate(nodeID, node.BandwidthDown)
		}

		return
	}

	if isV {
		m.removeValidator(nodeID)
	} else {
		m.removeBeValidate(nodeID)
	}
}
