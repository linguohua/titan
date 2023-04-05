package node

import (
	"crypto/rsa"
	"math/rand"
	"sync"
	"time"

	"github.com/filecoin-project/pubsub"
	"github.com/linguohua/titan/api/types"
	"github.com/linguohua/titan/node/modules/dtypes"

	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	"github.com/linguohua/titan/node/scheduler/db"
)

var log = logging.Logger("node")

const (
	offlineTimeMax = 24 // If it is not online after this time, it is determined that the node has quit the titan (unit: hour)

	keepaliveTime    = 30  // keepalive time interval (unit: second)
	saveInfoInterval = 10  // keepalive saves information every 10 times
	sizeOfBuckets    = 128 // The number of buckets in assets view
)

// Manager Node Manager
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

// NewManager return new node manager instance
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

func (m *Manager) run() {
	ticker := time.NewTicker(time.Duration(keepaliveTime) * time.Second)
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

func (m *Manager) storeEdge(node *Node) {
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

	m.notify.Pub(node, types.TopicsNodeOnline.String())
}

func (m *Manager) storeCandidate(node *Node) {
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

	m.notify.Pub(node, types.TopicsNodeOnline.String())
}

func (m *Manager) deleteEdge(node *Node) {
	m.repayEdgeSelectCode(node.selectCode)
	m.notify.Pub(node, types.TopicsNodeOffline.String())

	nodeID := node.NodeInfo.NodeID
	_, loaded := m.edgeNodes.LoadAndDelete(nodeID)
	if !loaded {
		return
	}
	m.Edges--
}

func (m *Manager) deleteCandidate(node *Node) {
	m.repayCandidateSelectCode(node.selectCode)
	m.notify.Pub(node, types.TopicsNodeOffline.String())

	nodeID := node.NodeInfo.NodeID
	_, loaded := m.candidateNodes.LoadAndDelete(nodeID)
	if !loaded {
		return
	}
	m.Candidates--
}

func (m *Manager) nodeKeepalive(node *Node, t time.Time, isSave bool) {
	lastTime := node.LastRequestTime()

	nodeID := node.NodeInfo.NodeID

	if !lastTime.After(t) {
		node.ClientCloser()
		if node.NodeType == types.NodeCandidate {
			m.deleteCandidate(node)
		} else if node.NodeType == types.NodeEdge {
			m.deleteEdge(node)
		}
		node = nil
		return
	}

	if isSave {
		err := m.UpdateNodeOnlineTime(nodeID, saveInfoInterval*keepaliveTime)
		if err != nil {
			log.Errorf("UpdateNodeOnlineTime err:%s,nodeID:%s", err.Error(), nodeID)
		}
	}
}

func (m *Manager) nodesKeepalive(isSave bool) {
	t := time.Now().Add(-time.Duration(keepaliveTime) * time.Second)

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

// KeepaliveCallBackFunc node keepalive call back
func KeepaliveCallBackFunc(nodeMgr *Manager) (dtypes.SessionCallbackFunc, error) {
	return func(nodeID, remoteAddr string) {
		lastTime := time.Now()

		node := nodeMgr.GetNode(nodeID)
		if node != nil {
			node.SetLastRequestTime(lastTime)
			err := node.ConnectRPC(remoteAddr, false, node.NodeType)
			if err != nil {
				log.Errorf("%s ConnectRPC err:%s", nodeID, err.Error())
			}
			return
		}
	}, nil
}

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

// node online
func (m *Manager) saveInfo(n *BaseInfo) error {
	n.Quitted = false
	n.LastTime = time.Now()

	err := m.UpsertNodeInfo(n.NodeInfo)
	if err != nil {
		return err
	}

	return nil
}

func (m *Manager) AddAsset(nodeID string, c cid.Cid) error {
	av := &AssetsView{}
	return av.addAsset(m, nodeID, c)
}

func (m *Manager) RemoveAsset(nodeID string, c cid.Cid) error {
	av := &AssetsView{}
	return av.removeAsset(m, nodeID, c)
}
