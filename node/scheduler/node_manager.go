package scheduler

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/node/scheduler/db/cache"
	"github.com/linguohua/titan/node/scheduler/db/persistent"
	"github.com/linguohua/titan/region"
	"github.com/ouqiang/timewheel"
	"golang.org/x/xerrors"
)

// node manager

type geoLevel int64

const (
	defaultLevel  geoLevel = 0
	countryLevel  geoLevel = 1
	provinceLevel geoLevel = 2
	cityLevel     geoLevel = 3
)

// NodeManager Node Manager
type NodeManager struct {
	edgeNodeMap      sync.Map
	candidateNodeMap sync.Map

	timewheelKeepalive *timewheel.TimeWheel
	keepaliveTime      int // keepalive time interval (minute)

	validatePool *ValidatePool

	areaManager *AreaManager
}

func newNodeManager(pool *ValidatePool) *NodeManager {
	nodeManager := &NodeManager{
		keepaliveTime: 1,
		validatePool:  pool,
		areaManager:   &AreaManager{},
	}

	nodeManager.initKeepaliveTimewheel()

	return nodeManager
}

// InitKeepaliveTimewheel ndoe Keepalive
func (m *NodeManager) initKeepaliveTimewheel() {
	m.timewheelKeepalive = timewheel.New(1*time.Second, 3600, func(_ interface{}) {
		m.nodeKeepalive()
		m.timewheelKeepalive.AddTimer(time.Duration(1)*60*time.Second, "Keepalive", nil)
	})
	m.timewheelKeepalive.Start()
	m.timewheelKeepalive.AddTimer(time.Duration(1)*60*time.Second, "Keepalive", nil)
}

func (m *NodeManager) nodeKeepalive() {
	nowTime := time.Now().Add(-time.Duration(m.keepaliveTime) * 60 * time.Second)

	m.edgeNodeMap.Range(func(key, value interface{}) bool {
		// deviceID := key.(string)
		node := value.(*EdgeNode)

		if node == nil {
			return true
		}

		lastTime := node.lastRequestTime

		if !lastTime.After(nowTime) {
			// offline
			m.removeEdgeNode(node)
			node = nil
			return true
		}

		// err := persistent.GetDB().AddNodeOnlineTime(deviceID, int64(m.keepaliveTime))
		// if err != nil {
		// 	log.Warnf("AddNodeOnlineTime err:%v,deviceID:%v", err.Error(), deviceID)
		// }

		return true
	})

	m.candidateNodeMap.Range(func(key, value interface{}) bool {
		// deviceID := key.(string)
		node := value.(*CandidateNode)

		if node == nil {
			return true
		}

		lastTime := node.lastRequestTime

		if !lastTime.After(nowTime) {
			// offline
			m.removeCandidateNode(node)
			node = nil
			return true
		}

		// err := persistent.GetDB().AddNodeOnlineTime(deviceID, int64(m.keepaliveTime))
		// if err != nil {
		// 	log.Warnf("AddNodeOnlineTime err:%v,deviceID:%v", err.Error(), deviceID)
		// }

		return true
	})

	err := persistent.GetDB().AddAllNodeOnlineTime(int64(m.keepaliveTime))
	if err != nil {
		log.Warnf("AddAllNodeOnlineTime err:%v", err.Error())
	}
}

func (m *NodeManager) addEdgeNode(node *EdgeNode) error {
	deviceID := node.deviceInfo.DeviceId
	// geo ip
	geoInfo, err := region.GetRegion().GetGeoInfo(node.deviceInfo.ExternalIp)
	if err != nil {
		log.Warnf("addEdgeNode GetGeoInfo err:%v,node:%v", err, node.deviceInfo.ExternalIp)
	}

	node.geoInfo = geoInfo

	nodeOld := m.getEdgeNode(deviceID)
	if nodeOld != nil {
		nodeOld.closer()

		nodeOld = nil
	}

	log.Infof("addEdgeNode DeviceId:%v,geo:%v", deviceID, node.geoInfo.Geo)

	err = node.online(0, api.TypeNameEdge)
	if err != nil {
		return err
	}

	m.edgeNodeMap.Store(deviceID, node)
	m.areaManager.addEdge(node)

	m.validatePool.addPendingNode(node, nil)

	return nil
}

func (m *NodeManager) getEdgeNode(deviceID string) *EdgeNode {
	nodeI, ok := m.edgeNodeMap.Load(deviceID)
	if ok && nodeI != nil {
		node := nodeI.(*EdgeNode)

		return node
	}

	return nil
}

func (m *NodeManager) removeEdgeNode(node *EdgeNode) {
	deviceID := node.deviceInfo.DeviceId
	// close old node
	node.closer()

	log.Warnf("removeEdgeNode :%v", deviceID)

	m.edgeNodeMap.Delete(deviceID)
	m.areaManager.removeEdge(node)

	m.validatePool.removeEdge(deviceID)

	node.offline(deviceID, node.geoInfo, api.TypeNameEdge, node.lastRequestTime)
}

func (m *NodeManager) addCandidateNode(node *CandidateNode) error {
	deviceID := node.deviceInfo.DeviceId

	// node.isValidator, _ = cache.GetDB().IsNodeInValidatorList(deviceID)

	// geo ip
	geoInfo, err := region.GetRegion().GetGeoInfo(node.deviceInfo.ExternalIp)
	if err != nil {
		log.Warnf("addCandidateNode GetGeoInfo err:%v,ExternalIp:%v", err, node.deviceInfo.ExternalIp)
	}

	node.geoInfo = geoInfo

	nodeOld := m.getCandidateNode(deviceID)
	if nodeOld != nil {
		nodeOld.closer()

		nodeOld = nil
	}

	log.Infof("addCandidateNode DeviceId:%v,geo:%v", deviceID, node.geoInfo.Geo)

	err = node.online(0, api.TypeNameCandidate)
	if err != nil {
		// log.Errorf("addCandidateNode NodeOnline err:%v", err)
		return err
	}

	m.candidateNodeMap.Store(deviceID, node)
	m.areaManager.addCandidate(node)

	m.validatePool.addPendingNode(nil, node)

	return nil
}

func (m *NodeManager) getCandidateNode(deviceID string) *CandidateNode {
	nodeI, ok := m.candidateNodeMap.Load(deviceID)
	if ok && nodeI != nil {
		node := nodeI.(*CandidateNode)

		return node
	}

	return nil
}

func (m *NodeManager) removeCandidateNode(node *CandidateNode) {
	deviceID := node.deviceInfo.DeviceId
	// close old node
	node.closer()

	log.Warnf("removeCandidateNode :%v", deviceID)

	m.candidateNodeMap.Delete(deviceID)
	m.areaManager.removeCandidate(node)

	m.validatePool.removeCandidate(deviceID)

	node.offline(deviceID, node.geoInfo, api.TypeNameCandidate, node.lastRequestTime)
}

func (m *NodeManager) findEdgeWithArea(key string, useDeviceIDs []string) []*EdgeNode {
	eMap := m.areaManager.getEdges(key)
	if eMap != nil && len(eMap) > 0 {
		list := make([]*EdgeNode, 0)

		if len(useDeviceIDs) > 0 {
			for _, dID := range useDeviceIDs {
				node, ok := eMap[dID]
				if ok {
					list = append(list, node)
				}
			}
		} else {
			for _, node := range eMap {
				list = append(list, node)
			}
		}

		if len(list) > 0 {
			return list
		}
	}

	return nil
}

func (m *NodeManager) findCandidateWithArea(key string, useDeviceIDs []string) []*CandidateNode {
	eMap := m.areaManager.getCandidates(key)
	if eMap != nil && len(eMap) > 0 {
		list := make([]*CandidateNode, 0)

		if len(useDeviceIDs) > 0 {
			for _, dID := range useDeviceIDs {
				node, ok := eMap[dID]
				if ok {
					list = append(list, node)
				}
			}
		} else {
			for _, node := range eMap {
				list = append(list, node)
			}
		}

		if len(list) > 0 {
			return list
		}
	}

	return nil
}

func (m *NodeManager) findEdgeNodeWithGeo(userGeoInfo *region.GeoInfo, useDeviceIDs []string) ([]*EdgeNode, geoLevel) {
	countryKey, provinceKey, cityKey, defaultKey := m.areaManager.getAreaKey(userGeoInfo)

	list := m.findEdgeWithArea(cityKey, useDeviceIDs)
	if list != nil {
		return list, cityLevel
	}

	list = m.findEdgeWithArea(provinceKey, useDeviceIDs)
	if list != nil {
		return list, provinceLevel
	}

	list = m.findEdgeWithArea(countryKey, useDeviceIDs)
	if list != nil {
		return list, countryLevel
	}

	list = m.findEdgeWithArea(defaultKey, useDeviceIDs)
	if list != nil {
		return list, defaultLevel
	}

	return nil, defaultLevel
}

func (m *NodeManager) findCandidateNodeWithGeo(userGeoInfo *region.GeoInfo, useDeviceIDs []string) ([]*CandidateNode, geoLevel) {
	countryKey, provinceKey, cityKey, defaultKey := m.areaManager.getAreaKey(userGeoInfo)

	list := m.findCandidateWithArea(cityKey, useDeviceIDs)
	if list != nil {
		return list, cityLevel
	}

	list = m.findCandidateWithArea(provinceKey, useDeviceIDs)
	if list != nil {
		return list, provinceLevel
	}

	list = m.findCandidateWithArea(countryKey, useDeviceIDs)
	if list != nil {
		return list, countryLevel
	}

	list = m.findCandidateWithArea(defaultKey, useDeviceIDs)
	if list != nil {
		return list, defaultLevel
	}

	return nil, defaultLevel
}

func (m *NodeManager) filterCandidates(filterDeviceIDs []string, nodes []*CandidateNode) []*CandidateNode {
	nodes2 := make([]*CandidateNode, 0)
	for _, node := range nodes {
		isHave := false
		for _, nd := range filterDeviceIDs {
			if node.deviceInfo.DeviceId == nd {
				isHave = true
			}
		}

		if !isHave {
			nodes2 = append(nodes2, node)
		}
	}

	return nodes2
}

// func (m *NodeManager) resetCandidateAndValidatorCount() {
// 	m.candidateCount = 0
// 	m.validatorCount = 0

// 	m.candidateNodeMap.Range(func(key, value interface{}) bool {
// 		node := value.(*CandidateNode)

// 		if node.isValidator {
// 			m.validatorCount++
// 		} else {
// 			m.candidateCount++
// 		}

// 		return true
// 	})
// }

func (m *NodeManager) updateLastRequestTime(deviceID string) {
	lastTime := time.Now()

	edge := m.getEdgeNode(deviceID)
	if edge != nil {
		edge.lastRequestTime = lastTime
		return
	}

	candidate := m.getCandidateNode(deviceID)
	if candidate != nil {
		candidate.lastRequestTime = lastTime
		return
	}
}

// getNodeURLWithData find device
func (m *NodeManager) nodeDownloadInfo(cid string, geoInfo *region.GeoInfo) (api.DownloadInfo, error) {
	var downloadInfo api.DownloadInfo
	deviceIDs, err := cache.GetDB().GetNodesWithCacheList(cid)
	if err != nil {
		return downloadInfo, err
	}

	if len(deviceIDs) <= 0 {
		return downloadInfo, xerrors.New(ErrNodeNotFind)
	}

	nodeEs, geoLevelE := m.findEdgeNodeWithGeo(geoInfo, deviceIDs)
	nodeCs, geoLevelC := m.findCandidateNodeWithGeo(geoInfo, deviceIDs)
	if geoLevelE < geoLevelC {
		node := nodeCs[randomNum(0, len(nodeCs))]
		return node.nodeAPI.GetDownloadInfo(context.Background())
	}

	if geoLevelE > geoLevelC {
		node := nodeCs[randomNum(0, len(nodeEs))]
		return node.nodeAPI.GetDownloadInfo(context.Background())
	}

	if len(nodeEs) > 0 {
		node := nodeCs[randomNum(0, len(nodeEs))]
		return node.nodeAPI.GetDownloadInfo(context.Background())
	}

	if len(nodeCs) > 0 {
		node := nodeCs[randomNum(0, len(nodeCs))]
		return node.nodeAPI.GetDownloadInfo(context.Background())
	}
	// http://192.168.0.136:3456/rpc/v0/block/get?cid=QmeUqw4FY1wqnh2FMvuc2v8KAapE7fYwu2Up4qNwhZiRk7

	return downloadInfo, xerrors.New(ErrNodeNotFind)
}

// getCandidateNodesWithData find device
func (m *NodeManager) getCandidateNodesWithData(cid string, geoInfo *region.GeoInfo) ([]*CandidateNode, error) {
	deviceIDs, err := cache.GetDB().GetNodesWithCacheList(cid)
	if err != nil {
		return nil, err
	}
	// log.Infof("getCandidateNodesWithData deviceIDs : %v", deviceIDs)

	if len(deviceIDs) <= 0 {
		return nil, xerrors.New(ErrNodeNotFind)
	}

	nodeCs, _ := m.findCandidateNodeWithGeo(geoInfo, deviceIDs)

	return nodeCs, nil
}

// AreaManager Node Area Manager
type AreaManager struct {
	edgeNodeMap      sync.Map
	candidateNodeMap sync.Map
}

func (a *AreaManager) getAreaKey(geoInfo *region.GeoInfo) (countryKey, provinceKey, cityKey, defaultKey string) {
	countryKey = geoInfo.Country
	provinceKey = fmt.Sprintf("%s-%s", geoInfo.Country, geoInfo.Province)
	cityKey = fmt.Sprintf("%s-%s-%s", geoInfo.Country, geoInfo.Province, geoInfo.City)

	defaultKey = "default"

	return
}

func (a *AreaManager) addEdge(node *EdgeNode) {
	countryKey, provinceKey, cityKey, defaultKey := a.getAreaKey(node.geoInfo)

	a.storeEdge(node, countryKey)
	a.storeEdge(node, provinceKey)
	a.storeEdge(node, cityKey)
	a.storeEdge(node, defaultKey)
}

func (a *AreaManager) removeEdge(node *EdgeNode) {
	countryKey, provinceKey, cityKey, defaultKey := a.getAreaKey(node.geoInfo)

	deviceID := node.deviceInfo.DeviceId

	a.deleteEdge(deviceID, countryKey)
	a.deleteEdge(deviceID, provinceKey)
	a.deleteEdge(deviceID, cityKey)
	a.deleteEdge(deviceID, defaultKey)
}

func (a *AreaManager) getEdges(key string) map[string]*EdgeNode {
	m, ok := a.edgeNodeMap.Load(key)
	if ok && m != nil {
		return m.(map[string]*EdgeNode)
	}

	return nil
}

func (a *AreaManager) addCandidate(node *CandidateNode) {
	countryKey, provinceKey, cityKey, defaultKey := a.getAreaKey(node.geoInfo)

	a.storeCandidate(node, countryKey)
	a.storeCandidate(node, provinceKey)
	a.storeCandidate(node, cityKey)
	a.storeCandidate(node, defaultKey)
}

func (a *AreaManager) removeCandidate(node *CandidateNode) {
	countryKey, provinceKey, cityKey, defaultKey := a.getAreaKey(node.geoInfo)

	deviceID := node.deviceInfo.DeviceId

	a.deleteCandidate(deviceID, countryKey)
	a.deleteCandidate(deviceID, provinceKey)
	a.deleteCandidate(deviceID, cityKey)
	a.deleteCandidate(deviceID, defaultKey)
}

func (a *AreaManager) getCandidates(key string) map[string]*CandidateNode {
	m, ok := a.candidateNodeMap.Load(key)
	if ok && m != nil {
		return m.(map[string]*CandidateNode)
	}

	return nil
}

func (a *AreaManager) storeEdge(node *EdgeNode, key string) {
	countryMap := make(map[string]*EdgeNode)
	m, ok := a.edgeNodeMap.Load(key)
	if ok && m != nil {
		countryMap = m.(map[string]*EdgeNode)
	}

	countryMap[node.deviceInfo.DeviceId] = node
	a.edgeNodeMap.Store(key, countryMap)
}

func (a *AreaManager) deleteEdge(deviceID string, key string) {
	m, ok := a.edgeNodeMap.Load(key)
	if ok && m != nil {
		nodeMap := m.(map[string]*EdgeNode)
		_, ok := nodeMap[deviceID]
		if ok {
			delete(nodeMap, deviceID)
			a.edgeNodeMap.Store(key, nodeMap)
		}
	}
}

func (a *AreaManager) storeCandidate(node *CandidateNode, key string) {
	countryMap := make(map[string]*CandidateNode)
	m, ok := a.edgeNodeMap.Load(key)
	if ok && m != nil {
		countryMap = m.(map[string]*CandidateNode)
	}

	countryMap[node.deviceInfo.DeviceId] = node
	a.edgeNodeMap.Store(key, countryMap)
}

func (a *AreaManager) deleteCandidate(deviceID string, key string) {
	m, ok := a.edgeNodeMap.Load(key)
	if ok && m != nil {
		nodeMap := m.(map[string]*CandidateNode)
		_, ok := nodeMap[deviceID]
		if ok {
			delete(nodeMap, deviceID)
			a.edgeNodeMap.Store(key, nodeMap)
		}
	}
}
