package scheduler

import (
	"sync"

	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/geoip"
)

var (
	edgeNodeMap      sync.Map
	candidateNodeMap sync.Map
)

func addEdgeNode(node *EdgeNode) {
	// geo ip
	geoInfo, err := geoip.GetGeoIP().GetGeoInfo(node.deviceInfo.ExternalIp)
	if err != nil {
		log.Errorf("addEdgeNode GetGeoInfo err : %v ,node : %v", err, node.deviceInfo.ExternalIp)
	}

	node.geoInfo = geoInfo

	nodeOld := getEdgeNode(node.deviceInfo.DeviceId)
	if nodeOld != nil {
		nodeOld.closer()
		// log.Infof("close old deviceID : %v ", nodeOld.deviceInfo.DeviceId)
	}

	log.Infof("addEdgeNode DeviceId:%v,geo:%v", node.deviceInfo.DeviceId, node.geoInfo.Geo)

	edgeNodeMap.Store(node.deviceInfo.DeviceId, node)

	err = nodeOnline(node.deviceInfo.DeviceId, 0, geoInfo, api.TypeNameEdge)
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

	err := nodeOffline(deviceID, node.geoInfo)
	if err != nil {
		log.Errorf("DeviceOffline err : %v ", err)
	}
}

func addCandidateNode(node *CandidateNode) {
	// geo ip
	geoInfo, err := geoip.GetGeoIP().GetGeoInfo(node.deviceInfo.ExternalIp)
	if err != nil {
		log.Errorf("addCandidateNode GetGeoInfo err : %v ,ExternalIp : %v", err, node.deviceInfo.ExternalIp)
	}

	node.geoInfo = geoInfo

	nodeOld := getCandidateNode(node.deviceInfo.DeviceId)
	if nodeOld != nil {
		nodeOld.closer()
		// log.Infof("close old deviceID : %v ", nodeOld.deviceInfo.DeviceId)
	}

	candidateNodeMap.Store(node.deviceInfo.DeviceId, node)

	log.Infof("addCandidateNode DeviceId:%v,geo:%v", node.deviceInfo.DeviceId, node.geoInfo.Geo)

	err = nodeOnline(node.deviceInfo.DeviceId, 0, geoInfo, api.TypeNameCandidate)
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

	err := nodeOffline(deviceID, node.geoInfo)
	if err != nil {
		log.Errorf("DeviceOffline err : %v ", err)
	}
}
