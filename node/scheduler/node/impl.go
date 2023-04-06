package node

import (
	"github.com/linguohua/titan/api/types"
)

// NodesQuit nodes quit and removes their replicas
func (m *Manager) NodesQuit(nodeIDs []string) {
	err := m.SetNodesQuitted(nodeIDs)
	if err != nil {
		log.Errorf("QuitNodes: SetNodesQuitted err:%s", err.Error())
		return
	}

	log.Infof("node event , nodes quit:%v", nodeIDs)

	hashes, err := m.FetchAssetHashesOfNodes(nodeIDs)
	if err != nil {
		log.Errorf("QuitNodes: FetchAssetHashesOfNodes err:%s", err.Error())
		return
	}

	err = m.DeleteReplicasForNodes(nodeIDs)
	if err != nil {
		log.Errorf("QuitNodes: DeleteReplicasForNodes err:%s", err.Error())
		return
	}

	for _, hash := range hashes {
		log.Infof("QuitNodes: Need to add replica for asset:%s", hash)
	}
}

// GetAllCandidateNodes  returns a list of all candidate nodes
func (m *Manager) GetAllCandidateNodes() []string {
	var out []string
	m.candidateNodes.Range(func(key, value interface{}) bool {
		nodeID := key.(string)
		out = append(out, nodeID)
		return true
	})

	return out
}

// GetNode retrieves a node with the given node ID
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

// GetEdgeNode retrieves an edge node with the given node ID
func (m *Manager) GetEdgeNode(nodeID string) *Node {
	nodeI, exist := m.edgeNodes.Load(nodeID)
	if exist && nodeI != nil {
		node := nodeI.(*Node)

		return node
	}

	return nil
}

// GetCandidateNode retrieves a candidate node with the given node ID
func (m *Manager) GetCandidateNode(nodeID string) *Node {
	nodeI, exist := m.candidateNodes.Load(nodeID)
	if exist && nodeI != nil {
		node := nodeI.(*Node)

		return node
	}

	return nil
}

// GetOnlineNodeList returns a list of online nodes of the given type
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

// NodeOnline registers a node as online
func (m *Manager) NodeOnline(node *Node) error {
	nodeID := node.NodeInfo.NodeID

	err := m.saveInfo(node.BaseInfo)
	if err != nil {
		return err
	}

	if node.NodeType == types.NodeEdge {
		m.storeEdgeNode(node)
		return nil
	}

	if node.NodeType == types.NodeCandidate {
		m.storeCandidateNode(node)
		// update validator owner
		return m.UpdateValidatorInfo(m.ServerID, nodeID)
	}

	return nil
}
