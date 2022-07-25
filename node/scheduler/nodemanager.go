package scheduler

import (
	"sync"
	"time"

	"titan/node/scheduler/redishelper"
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

	err := redishelper.SaveDeciceInfo(edgeNode.deviceID, time.Now().Format("2006-01-02 15:04:05"))
	if err != nil {
		log.Errorf("SaveDeciceInfo err : %v ", err)
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
}
