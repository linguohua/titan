package node

import (
	"math"
	"math/rand"
	"time"

	"github.com/linguohua/titan/api/types"
)

const maxRetryCount = 3

// SelectCandidateToPullAsset select candidate node to pull asset replica
func (m *Manager) SelectCandidateToPullAsset(count int, filterNodes []string) map[string]*Node {
	selectMap := make(map[string]*Node)
	if count <= 0 {
		return selectMap
	}

	if len(filterNodes) >= len(m.cDistributedSelectCode) {
		return selectMap
	}

	filterMap := make(map[string]struct{})
	for _, nodeID := range filterNodes {
		filterMap[nodeID] = struct{}{}
	}

	num := count * maxRetryCount

	for i := 0; i < num; i++ {
		selectCode := m.getSelectCodeRandom(m.cPullSelectCode, m.cPullSelectRand)
		nodeID, exist := m.cDistributedSelectCode[selectCode]
		if !exist {
			continue
		}

		node := m.GetCandidateNode(nodeID)
		if node == nil {
			continue
		}

		if _, exist := filterMap[nodeID]; exist {
			continue
		}

		if node.DiskUsage > maxNodeDiskUsage {
			continue
		}

		selectMap[nodeID] = node
		if len(selectMap) >= count {
			break
		}
	}

	return selectMap
}

// SelectEdgeToPullAsset select edge node to pull asset replica
func (m *Manager) SelectEdgeToPullAsset(count int, filterNodes []string) map[string]*Node {
	selectMap := make(map[string]*Node)
	if count <= 0 {
		return selectMap
	}

	if len(filterNodes) >= len(m.eDistributedSelectCode) {
		return selectMap
	}

	filterMap := make(map[string]struct{})
	for _, nodeID := range filterNodes {
		filterMap[nodeID] = struct{}{}
	}

	for i := 0; i < count*maxRetryCount; i++ {
		selectCode := m.getSelectCodeRandom(m.ePullSelectCode, m.ePullSelectRand)
		nodeID, exist := m.eDistributedSelectCode[selectCode]
		if !exist {
			continue
		}

		node := m.GetEdgeNode(nodeID)
		if node == nil {
			continue
		}

		if _, exist := filterMap[nodeID]; exist {
			continue
		}

		if node.DiskUsage > maxNodeDiskUsage {
			continue
		}

		selectMap[nodeID] = node
		if len(selectMap) >= count {
			break
		}
	}

	return selectMap
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

	needValidatorCount := int(math.Ceil(float64(m.candidates) * ratio))
	if needValidatorCount <= 0 {
		return
	}

	m.candidateNodes.Range(func(key, value interface{}) bool {
		nodeID := key.(string)
		out = append(out, nodeID)
		return true
	})

	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(out), func(i, j int) {
		out[i], out[j] = out[j], out[i]
	})

	if needValidatorCount > len(out) {
		needValidatorCount = len(out)
	}

	out = out[:needValidatorCount]

	return
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

// OnlineNodeList get nodes with type
func (m *Manager) OnlineNodeList(nodeType types.NodeType) ([]string, error) {
	list := make([]string, 0)
	limit := 100

	i := 0
	if nodeType == types.NodeUnknown || nodeType == types.NodeCandidate {
		m.candidateNodes.Range(func(key, value interface{}) bool {
			nodeID := key.(string)
			list = append(list, nodeID)

			if i >= limit {
				return false
			}
			i++

			return true
		})
	}

	if nodeType == types.NodeUnknown || nodeType == types.NodeEdge {
		m.edgeNodes.Range(func(key, value interface{}) bool {
			nodeID := key.(string)
			list = append(list, nodeID)

			if i >= limit {
				return false
			}
			i++

			return true
		})
	}

	return list, nil
}

// NodeOnline node online
func (m *Manager) NodeOnline(node *Node) error {
	nodeID := node.NodeInfo.NodeID

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
