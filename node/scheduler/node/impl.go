package node

import (
	"github.com/linguohua/titan/api/types"
)

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

// GetAllCandidates get all candidates
func (m *Manager) GetAllCandidates() []string {
	var out []string
	m.candidateNodes.Range(func(key, value interface{}) bool {
		nodeID := key.(string)
		out = append(out, nodeID)
		return true
	})

	return out
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

// GetOnlineNodeList get nodes with type
func (m *Manager) GetOnlineNodeList(nodeType types.NodeType) ([]string, error) {
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
