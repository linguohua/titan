package scheduler

import "sync"

var edgeNodeMap sync.Map

func addEdgeNode(edgeNode *EdgeNode) {
	nodeI, ok := edgeNodeMap.Load(edgeNode.deviceID)
	log.Infof("addEdgeNode load : %v , %v", edgeNode.deviceID, ok)
	if ok && nodeI != nil {
		node := nodeI.(*EdgeNode)
		// close old node
		node.closer()

		log.Infof("addEdgeNode old addr : %v ", node.addr)
	}

	edgeNodeMap.Store(edgeNode.deviceID, edgeNode)
}

func getEdgeNode(deviceID string) *EdgeNode {
	nodeI, ok := edgeNodeMap.Load(deviceID)
	if ok && nodeI != nil {
		node := nodeI.(*EdgeNode)

		return node
	}

	return nil
}
