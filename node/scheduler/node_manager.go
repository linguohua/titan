package scheduler

import (
	"sync"

	"github.com/linguohua/titan/geoip"
)

var (
	edgeNodeMap      sync.Map
	candidateNodeMap sync.Map
)

func addEdgeNode(node *EdgeNode) {
	// geo ip
	geoInfo, err := geoip.GetGeoIP().GetGeoInfo(node.ip)
	if err != nil {
		log.Errorf("addEdgeNode GetGeoInfo err : %v ,node : %v", err, node.deviceID)
	}

	node.geoInfo = geoInfo

	nodeOld := getEdgeNode(node.deviceID)
	if node != nil {
		nodeOld.closer()
		log.Infof("close old deviceID : %v ", nodeOld.deviceID)
	}

	edgeNodeMap.Store(node.deviceID, node)

	err = NodeOnline(node.deviceID, 0, geoInfo)
	if err != nil {
		log.Errorf("addEdgeNode NodeOnline err : %v ", err)
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
	node := getEdgeNode(deviceID)
	if node == nil {
		return
	}

	// close old node
	node.closer()

	edgeNodeMap.Delete(deviceID)

	err := NodeOffline(deviceID, node.geoInfo)
	if err != nil {
		log.Errorf("DeviceOffline err : %v ", err)
	}
}

// func addCandidateNode(node *CandidateNode) {
// 	nodeI, ok := candidateNodeMap.Load(node.deviceID)
// 	// log.Infof("addEdgeNode load : %v , %v", edgeNode.deviceID, ok)
// 	if ok && nodeI != nil {
// 		node := nodeI.(*EdgeNode)
// 		// close old node
// 		node.closer()

// 		log.Infof("close old deviceID : %v ", node.deviceID)
// 	}

// 	candidateNodeMap.Store(node.deviceID, node)

// 	err := NodeOnline(node.deviceID, 0)
// 	if err != nil {
// 		log.Errorf("DeviceOnline err : %v ", err)
// 	}
// }

// func getCandidateNode(deviceID string) *CandidateNode {
// 	nodeI, ok := candidateNodeMap.Load(deviceID)
// 	if ok && nodeI != nil {
// 		node := nodeI.(*CandidateNode)

// 		return node
// 	}

// 	return nil
// }

// func deleteCandidateNode(deviceID string) {
// 	nodeI, ok := candidateNodeMap.Load(deviceID)
// 	if ok && nodeI != nil {
// 		node := nodeI.(*CandidateNode)

// 		// close old node
// 		node.closer()
// 	}

// 	candidateNodeMap.Delete(deviceID)

// 	err := NodeOffline(deviceID)
// 	if err != nil {
// 		log.Errorf("DeviceOffline err : %v ", err)
// 	}
// }
