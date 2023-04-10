package node

import (
	"crypto/rsa"
	"math/rand"
	"sync"
	"time"

	"github.com/filecoin-project/pubsub"
	"github.com/linguohua/titan/api/types"
	"github.com/linguohua/titan/node/modules/dtypes"

	logging "github.com/ipfs/go-log/v2"
	"github.com/linguohua/titan/node/scheduler/db"
)

var log = logging.Logger("node")

const (
	// offlineTimeMax is the maximum amount of time a node can be offline before being considered as quit
	offlineTimeMax = 24 // hours

	//  keepaliveTime is the interval between keepalive requests
	keepaliveTime = 30 * time.Second // seconds

	// saveInfoInterval is the interval at which node information is saved during keepalive requests
	saveInfoInterval = 10 // keepalive saves information every 10 times
)

// Manager is the node manager responsible for managing the online nodes
type Manager struct {
	edgeNodes      sync.Map
	candidateNodes sync.Map
	Edges          int // online edge node count
	Candidates     int // online candidate node count

	notify *pubsub.PubSub
	*db.SQLDB
	*rsa.PrivateKey
	dtypes.ServerID

	// Each node can be assigned a select code, when pulling resources, randomly select n select codes, and select the node holding these select code.
	selectCodeLock           sync.RWMutex
	cPullSelectRand          *rand.Rand     // The rand of select candidate nodes to pull asset
	ePullSelectRand          *rand.Rand     // The rand of select edge nodes to pull asset
	cPullSelectCode          int            // Candidate node select code , Distribute from 1
	ePullSelectCode          int            // Edge node select code , Distribute from 1
	cDistributedSelectCode   map[int]string // Distributed candidate node select codes
	cUndistributedSelectCode map[int]string // Undistributed candidate node select codes
	eDistributedSelectCode   map[int]string // Distributed edge node select codes
	eUndistributedSelectCode map[int]string // Undistributed edge node select codes
}

// NewManager creates a new instance of the node manager
func NewManager(sdb *db.SQLDB, serverID dtypes.ServerID, k *rsa.PrivateKey, p *pubsub.PubSub) *Manager {
	pullSelectSeed := time.Now().UnixNano()

	nodeManager := &Manager{
		SQLDB:      sdb,
		ServerID:   serverID,
		PrivateKey: k,
		notify:     p,

		cPullSelectRand:          rand.New(rand.NewSource(pullSelectSeed)),
		ePullSelectRand:          rand.New(rand.NewSource(pullSelectSeed)),
		cDistributedSelectCode:   make(map[int]string),
		cUndistributedSelectCode: make(map[int]string),
		eDistributedSelectCode:   make(map[int]string),
		eUndistributedSelectCode: make(map[int]string),
	}

	go nodeManager.run()

	return nodeManager
}

// run periodically sends keepalive requests to all nodes and checks if any nodes have been offline for too long
func (m *Manager) run() {
	ticker := time.NewTicker(keepaliveTime)
	defer ticker.Stop()

	count := 0

	for {
		<-ticker.C
		count++
		saveInfo := count%saveInfoInterval == 0
		m.nodesKeepalive(saveInfo)
		// Check how long a node has been offline
		m.checkNodesTTL()
	}
}

// storeEdgeNode adds an edge node to the manager's list of edge nodes
func (m *Manager) storeEdgeNode(node *Node) {
	if node == nil {
		return
	}
	nodeID := node.NodeInfo.NodeID
	_, loaded := m.edgeNodes.LoadOrStore(nodeID, node)
	if loaded {
		return
	}
	m.Edges++

	code := m.distributeEdgeSelectCode(nodeID)
	node.selectCode = code

	m.notify.Pub(node, types.EventNodeOnline.String())
}

// adds a candidate node to the manager's list of candidate nodes
func (m *Manager) storeCandidateNode(node *Node) {
	if node == nil {
		return
	}

	nodeID := node.NodeInfo.NodeID
	_, loaded := m.candidateNodes.LoadOrStore(nodeID, node)
	if loaded {
		return
	}
	m.Candidates++

	code := m.distributeCandidateSelectCode(nodeID)
	node.selectCode = code

	m.notify.Pub(node, types.EventNodeOnline.String())
}

// deleteEdgeNode removes an edge node from the manager's list of edge nodes
func (m *Manager) deleteEdgeNode(node *Node) {
	m.repayEdgeSelectCode(node.selectCode)
	m.notify.Pub(node, types.EventNodeOffline.String())

	nodeID := node.NodeInfo.NodeID
	_, loaded := m.edgeNodes.LoadAndDelete(nodeID)
	if !loaded {
		return
	}
	m.Edges--
}

// deleteCandidateNode removes a candidate node from the manager's list of candidate nodes
func (m *Manager) deleteCandidateNode(node *Node) {
	m.repayCandidateSelectCode(node.selectCode)
	m.notify.Pub(node, types.EventNodeOffline.String())

	nodeID := node.NodeInfo.NodeID
	_, loaded := m.candidateNodes.LoadAndDelete(nodeID)
	if !loaded {
		return
	}
	m.Candidates--
}

// nodeKeepalive checks if a node has sent a keepalive recently and updates node status accordingly
func (m *Manager) nodeKeepalive(node *Node, t time.Time, isSave bool) {
	lastTime := node.LastRequestTime()

	nodeID := node.NodeInfo.NodeID

	if !lastTime.After(t) {
		node.ClientCloser()
		if node.Type == types.NodeCandidate {
			m.deleteCandidateNode(node)
		} else if node.Type == types.NodeEdge {
			m.deleteEdgeNode(node)
		}
		node = nil
		return
	}

	if isSave {
		node.OnlineDuration += int((saveInfoInterval * keepaliveTime) / time.Minute)

		err := m.UpdateNodeOnlineTime(nodeID, node.OnlineDuration)
		if err != nil {
			log.Errorf("UpdateNodeOnlineTime err:%s,nodeID:%s", err.Error(), nodeID)
		}
	}
}

// nodesKeepalive checks all nodes in the manager's lists for keepalive
func (m *Manager) nodesKeepalive(isSave bool) {
	t := time.Now().Add(-keepaliveTime)

	m.edgeNodes.Range(func(key, value interface{}) bool {
		node := value.(*Node)
		if node == nil {
			return true
		}

		go m.nodeKeepalive(node, t, isSave)

		return true
	})

	m.candidateNodes.Range(func(key, value interface{}) bool {
		node := value.(*Node)
		if node == nil {
			return true
		}

		go m.nodeKeepalive(node, t, isSave)

		return true
	})
}

// KeepaliveCallBackFunc Callback function to handle node keepalive
func KeepaliveCallBackFunc(nodeMgr *Manager) (dtypes.SessionCallbackFunc, error) {
	return func(nodeID, remoteAddr string) {
		lastTime := time.Now()

		node := nodeMgr.GetNode(nodeID)
		if node != nil {
			node.SetLastRequestTime(lastTime)
			err := node.ConnectRPC(remoteAddr, false, node.Type)
			if err != nil {
				log.Errorf("%s ConnectRPC err:%s", nodeID, err.Error())
			}
			return
		}
	}, nil
}

// checkNodesTTL Check for nodes that have timed out and quit them
func (m *Manager) checkNodesTTL() {
	nodes, err := m.LoadTimeoutNodes(offlineTimeMax, m.ServerID)
	if err != nil {
		log.Errorf("checkWhetherNodeQuits LoadTimeoutNodes err:%s", err.Error())
		return
	}

	if len(nodes) > 0 {
		m.NodesQuit(nodes)
	}
}

// saveInfo Save node information when it comes online
func (m *Manager) saveInfo(n *BaseInfo) error {
	n.IsQuitted = false
	n.LastSeen = time.Now()

	err := m.SaveNodeInfo(n.NodeInfo)
	if err != nil {
		return err
	}

	return nil
}
