package scheduler

import (
	"sync"
)

var edgeNodeMap sync.Map

func addEdgeNode(edgeNode *EdgeNode) {
	nodeI, ok := edgeNodeMap.Load(edgeNode.deviceID)
	// log.Infof("addEdgeNode load : %v , %v", edgeNode.deviceID, ok)
	if ok && nodeI != nil {
		node := nodeI.(*EdgeNode)
		// close old node
		node.closer()

		log.Infof("close old deviceID : %v ", node.deviceID)
	}

	edgeNodeMap.Store(edgeNode.deviceID, edgeNode)

	err := DeviceOnline(edgeNode.deviceID, 0)
	if err != nil {
		log.Errorf("DeviceOnline err : %v ", err)
	}
}

func getEdgeNode(deviceID string) *EdgeNode {
	nodeI, ok := edgeNodeMap.Load(deviceID)
	if ok && nodeI != nil {
		node := nodeI.(*EdgeNode)

		return node
	}

	return nil
}

func deleteEdgeNode(deviceID string) {
	nodeI, ok := edgeNodeMap.Load(deviceID)
	if ok && nodeI != nil {
		node := nodeI.(*EdgeNode)

		// close old node
		node.closer()
	}

	edgeNodeMap.Delete(deviceID)

	err := DeviceOffline(deviceID)
	if err != nil {
		log.Errorf("DeviceOffline err : %v ", err)
	}
}
