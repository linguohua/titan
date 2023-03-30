package node

import (
	"crypto/rsa"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/linguohua/titan/api/types"
	"github.com/linguohua/titan/node/modules/dtypes"

	logging "github.com/ipfs/go-log/v2"
	"github.com/linguohua/titan/node/scheduler/db"
)

var log = logging.Logger("node")

const (
	offlineTimeMax = 24 // If it is not online after this time, it is determined that the node has quit the titan (unit: hour)

	keepaliveTime    = 30 // keepalive time interval (unit: second)
	saveInfoInterval = 10 // keepalive saves information every 10 times

	maxNodeDiskUsage = 95.0 // If the node disk size is greater than this value, pulling will not continue
)

// Manager Node Manager
type Manager struct {
	edgeNodes      sync.Map
	candidateNodes sync.Map
	*db.SQLDB

	*rsa.PrivateKey
	dtypes.ServerID
}

// NewManager return new node manager instance
func NewManager(sdb *db.SQLDB, serverID dtypes.ServerID, k *rsa.PrivateKey) *Manager {
	nodeManager := &Manager{
		SQLDB:      sdb,
		ServerID:   serverID,
		PrivateKey: k,
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

func (m *Manager) nodeKeepalive(node *Node, t time.Time, isSave bool) {
	lastTime := node.LastRequestTime()

	nodeID := node.NodeInfo.NodeID

	if !lastTime.After(t) {
		node.closer()
		if node.NodeType == types.NodeCandidate {
			m.deleteCandidate(nodeID)
		} else if node.NodeType == types.NodeEdge {
			m.deleteEdge(nodeID)
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

// OnlineNodeList get nodes with type
func (m *Manager) OnlineNodeList(nodeType types.NodeType) ([]string, error) {
	list := make([]string, 0)

	// TODO problematic
	if nodeType == types.NodeUnknown || nodeType == types.NodeCandidate {
		m.candidateNodes.Range(func(key, value interface{}) bool {
			nodeID := key.(string)
			list = append(list, nodeID)

			return true
		})
	}

	if nodeType == types.NodeUnknown || nodeType == types.NodeEdge {
		m.edgeNodes.Range(func(key, value interface{}) bool {
			nodeID := key.(string)
			list = append(list, nodeID)

			return true
		})
	}

	return list, nil
}

// NodeOnline node online
func (m *Manager) NodeOnline(node *Node) error {
	nodeID := node.NodeInfo.NodeID

	nodeOld := m.GetNode(nodeID)
	if nodeOld != nil {
		nodeOld.closer()

		nodeOld = nil
	}

	err := m.saveInfo(node.BaseInfo)
	if err != nil {
		return err
	}

	if node.NodeType == types.NodeEdge {
		m.storeEdge(node)
		return nil
	}

	if node.NodeType == types.NodeCandidate {
		m.storeCandidate(node)
		// update validator owner
		return m.UpdateValidatorInfo(m.ServerID, nodeID)
	}

	return nil
}

func (m *Manager) storeEdge(node *Node) {
	if node == nil {
		return
	}
	m.edgeNodes.Store(node.NodeInfo.NodeID, node)
}

func (m *Manager) storeCandidate(node *Node) {
	if node == nil {
		return
	}
	m.candidateNodes.Store(node.NodeInfo.NodeID, node)
}

func (m *Manager) deleteEdge(nodeID string) {
	m.edgeNodes.Delete(nodeID)
}

func (m *Manager) deleteCandidate(nodeID string) {
	m.candidateNodes.Delete(nodeID)
}

// GetEdgeNode Get EdgeNode
func (m *Manager) GetEdgeNode(nodeID string) *Node {
	nodeI, exist := m.edgeNodes.Load(nodeID)
	if exist && nodeI != nil {
		node := nodeI.(*Node)

		return node
	}

	return nil
}

// GetCandidateNode Get Candidate Node
func (m *Manager) GetCandidateNode(nodeID string) *Node {
	nodeI, exist := m.candidateNodes.Load(nodeID)
	if exist && nodeI != nil {
		node := nodeI.(*Node)

		return node
	}

	return nil
}

// KeepaliveCallBackFunc node keepalive call back
func KeepaliveCallBackFunc(nodeMgr *Manager) (dtypes.SessionCallbackFunc, error) {
	return func(nodeID, remoteAddr string) {
		lastTime := time.Now()

		node := nodeMgr.GetNode(nodeID)
		if node != nil {
			node.SetLastRequestTime(lastTime)
			_, err := node.ConnectRPC(remoteAddr, false, node.NodeType)
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

// NodesQuit Nodes quit
func (m *Manager) NodesQuit(nodeIDs []string) {
	err := m.SetNodesQuitted(nodeIDs)
	if err != nil {
		log.Errorf("NodeExited SetNodesQuit err:%s", err.Error())
		return
	}

	log.Infof("node event , nodes quit:%v", nodeIDs)

	hashes, err := m.LoadAssetHashesOfNodes(nodeIDs)
	if err != nil {
		log.Errorf("LoadAssetHashesOfNodes err:%s", err.Error())
		return
	}

	err = m.RemoveReplicasOfNodes(nodeIDs)
	if err != nil {
		log.Errorf("RemoveReplicaInfoWithNodes err:%s", err.Error())
		return
	}

	for _, hash := range hashes {
		log.Infof("need to add replica :%s", hash)
	}
}

// GetNode get node
func (m *Manager) GetNode(nodeID string) *Node {
	edge := m.GetEdgeNode(nodeID)
	if edge != nil {
		return edge
	}

	candidate := m.GetCandidateNode(nodeID)
	if candidate != nil {
		return candidate
	}

	return nil
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

// SelectCandidateToPullAsset select candidate node to pull asset replica
func (m *Manager) SelectCandidateToPullAsset(count int, filterNodes []string) []*Node {
	list := make([]*Node, 0)

	if count <= 0 {
		return list
	}

	filterMap := make(map[string]struct{})
	for _, nodeID := range filterNodes {
		filterMap[nodeID] = struct{}{}
	}

	// TODO problematic  need tactics
	m.candidateNodes.Range(func(key, value interface{}) bool {
		candidateNode := value.(*Node)

		if _, exist := filterMap[candidateNode.NodeInfo.NodeID]; exist {
			return true
		}

		if candidateNode.DiskUsage > maxNodeDiskUsage {
			return true
		}

		list = append(list, candidateNode)
		if len(list) >= count {
			return false
		}

		return true
	})

	return list
}

// SelectEdgeToPullAsset select edge node to pull asset replica
func (m *Manager) SelectEdgeToPullAsset(count int, filterNodes []string) []*Node {
	list := make([]*Node, 0)

	if count <= 0 {
		return list
	}

	filterMap := make(map[string]struct{})
	for _, nodeID := range filterNodes {
		filterMap[nodeID] = struct{}{}
	}

	// TODO problematic  need tactics
	m.edgeNodes.Range(func(key, value interface{}) bool {
		edgeNode := value.(*Node)

		if _, exist := filterMap[edgeNode.NodeInfo.NodeID]; exist {
			return true
		}

		if edgeNode.DiskUsage > maxNodeDiskUsage {
			return true
		}

		list = append(list, edgeNode)
		if len(list) >= count {
			return false
		}

		return true
	})

	return list
}

// RangeEdges range edges
func (m *Manager) RangeEdges(back func(key, value interface{}) bool) {
	// TODO problematic
	m.edgeNodes.Range(func(key, value interface{}) bool {
		return back(key, value)
	})
}

// RangeCandidates range candidate
func (m *Manager) RangeCandidates(back func(key, value interface{}) bool) {
	// TODO problematic
	m.candidateNodes.Range(func(key, value interface{}) bool {
		return back(key, value)
	})
}

// ElectValidators elect
func (m *Manager) ElectValidators(ratio float64) (out []string) {
	out = make([]string, 0)

	// TODO problematic
	candidates := make([]*Node, 0)
	m.candidateNodes.Range(func(key, value interface{}) bool {
		n := value.(*Node)
		candidates = append(candidates, n)
		return true
	})

	candidateCount := len(candidates)

	needValidatorCount := int(math.Ceil(float64(candidateCount) * ratio))
	if needValidatorCount <= 0 {
		return
	}

	if needValidatorCount >= candidateCount {
		for _, candidate := range candidates {
			out = append(out, candidate.NodeInfo.NodeID)
		}
		return
	}

	sort.Slice(candidates, func(i, j int) bool {
		// TODO Consider node reliability
		return candidates[i].BandwidthDown > candidates[j].BandwidthDown
	})

	for i := 0; i < needValidatorCount; i++ {
		candidate := candidates[i]
		out = append(out, candidate.NodeInfo.NodeID)
	}

	return out
}
