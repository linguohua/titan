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

func addCandidateNode(node *CandidateNode) {
	// geo ip
	geoInfo, err := geoip.GetGeoIP().GetGeoInfo(node.ip)
	if err != nil {
		log.Errorf("addCandidateNode GetGeoInfo err : %v ,node : %v", err, node.deviceID)
	}

	node.geoInfo = geoInfo

	nodeOld := getCandidateNode(node.deviceID)
	if node != nil {
		nodeOld.closer()
		log.Infof("close old deviceID : %v ", nodeOld.deviceID)
	}

	candidateNodeMap.Store(node.deviceID, node)

	err = NodeOnline(node.deviceID, 0, geoInfo)
	if err != nil {
		log.Errorf("addCandidateNode NodeOnline err : %v ", err)
	}
}

func getCandidateNode(deviceID string) *CandidateNode {
	nodeI, ok := candidateNodeMap.Load(deviceID)
	if ok && nodeI != nil {
		node := nodeI.(*CandidateNode)

		return node
	}

	return nil
}

func deleteCandidateNode(deviceID string) {
	node := getCandidateNode(deviceID)
	if node == nil {
		return
	}

	// close old node
	node.closer()

	candidateNodeMap.Delete(deviceID)

	err := NodeOffline(deviceID, node.geoInfo)
	if err != nil {
		log.Errorf("DeviceOffline err : %v ", err)
	}
}
