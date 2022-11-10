package scheduler

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/node/scheduler/db/cache"
	"github.com/linguohua/titan/node/scheduler/db/persistent"
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

	defaultKey = "default"
)

// NodeManager Node Manager
type NodeManager struct {
	edgeNodeMap      sync.Map
	candidateNodeMap sync.Map

	timewheelKeepalive *timewheel.TimeWheel
	keepaliveTime      float64 // keepalive time interval (minute)

	validatePool   *ValidatePool
	locatorManager *LocatorManager

	state api.StateNetwork
	// areaManager *AreaManager
}

func newNodeManager(pool *ValidatePool, locatorManager *LocatorManager) *NodeManager {
	nodeManager := &NodeManager{
		keepaliveTime:  0.5,
		validatePool:   pool,
		locatorManager: locatorManager,
		// areaManager:   &AreaManager{},
	}

	nodeManager.initKeepaliveTimewheel()

	return nodeManager
}

// InitKeepaliveTimewheel ndoe Keepalive
func (m *NodeManager) initKeepaliveTimewheel() {
	m.timewheelKeepalive = timewheel.New(1*time.Second, 3600, func(_ interface{}) {
		m.timewheelKeepalive.AddTimer(time.Duration(m.keepaliveTime*60-1)*time.Second, "Keepalive", nil)
		m.nodeKeepalive()
	})
	m.timewheelKeepalive.Start()
	m.timewheelKeepalive.AddTimer(time.Duration(m.keepaliveTime*60)*time.Second, "Keepalive", nil)
}

func (m *NodeManager) nodeKeepalive() {
	nowTime := time.Now().Add(-time.Duration(m.keepaliveTime*60) * time.Second)
	// log.Warnf("nodeKeepalive nowTime :%s", nowTime.String())

	m.edgeNodeMap.Range(func(key, value interface{}) bool {
		deviceID := key.(string)
		node := value.(*EdgeNode)

		if node == nil {
			return true
		}

		lastTime := node.lastRequestTime
		// log.Warnf("%s, lastTime :%s", deviceID, lastTime.String())

		if !lastTime.After(nowTime) {
			// offline
			m.edgeOffline(node)
			node = nil
			return true
		}

		_, err := cache.GetDB().IncrNodeOnlineTime(deviceID, m.keepaliveTime)
		if err != nil {
			log.Warnf("IncrNodeOnlineTime err:%s,deviceID:%s", err.Error(), deviceID)
		}

		return true
	})

	m.candidateNodeMap.Range(func(key, value interface{}) bool {
		deviceID := key.(string)
		node := value.(*CandidateNode)

		if node == nil {
			return true
		}

		lastTime := node.lastRequestTime

		if !lastTime.After(nowTime) {
			// offline
			m.candidateOffline(node)
			node = nil
			return true
		}

		_, err := cache.GetDB().IncrNodeOnlineTime(deviceID, m.keepaliveTime)
		if err != nil {
			log.Warnf("IncrNodeOnlineTime err:%s,deviceID:%s", err.Error(), deviceID)
		}

		return true
	})

	// err := persistent.GetDB().AddAllNodeOnlineTime(int64(m.keepaliveTime))
	// if err != nil {
	// 	log.Warnf("AddAllNodeOnlineTime err:%v", err.Error())
	// }
}

func (m *NodeManager) edgeOnline(node *EdgeNode) error {
	deviceID := node.deviceInfo.DeviceId
	// geo ip
	// geoInfo, _ := region.GetRegion().GetGeoInfo(node.deviceInfo.ExternalIp)
	// if err != nil {
	// 	log.Warnf("edgeOnline GetGeoInfo err:%v,node:%v", err, node.deviceInfo.ExternalIp)
	// }

	isOk, geoInfo := ipLegality(node.deviceInfo.ExternalIp)
	if !isOk {
		log.Errorf("edgeOnline err DeviceId:%s,ip%s,geo:%s", deviceID, node.deviceInfo.ExternalIp, geoInfo.Geo)
		return xerrors.Errorf(ErrAreaNotExist, geoInfo.Geo, node.deviceInfo.ExternalIp)
	}

	node.geoInfo = geoInfo

	nodeOld := m.getEdgeNode(deviceID)
	if nodeOld != nil {
		nodeOld.closer()

		nodeOld = nil
	}

	err := node.setNodeOnline(api.TypeNameEdge)
	if err != nil {
		return err
	}

	m.edgeNodeMap.Store(deviceID, node)
	// m.areaManager.addEdge(node)

	m.validatePool.addPendingNode(node, nil)

	m.StateNetwork()

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

func (m *NodeManager) getNodeArea(deviceID string) string {
	e := m.getEdgeNode(deviceID)
	if e != nil {
		return e.geoInfo.Geo
	}

	c := m.getCandidateNode(deviceID)
	if c != nil {
		return c.geoInfo.Geo
	}

	return ""
}

func (m *NodeManager) edgeOffline(node *EdgeNode) {
	deviceID := node.deviceInfo.DeviceId
	// close old node
	node.closer()

	log.Warnf("edgeOffline :%s", deviceID)

	m.edgeNodeMap.Delete(deviceID)
	// m.areaManager.removeEdge(node)
	m.validatePool.removeEdge(deviceID)

	node.setNodeOffline(deviceID, node.geoInfo, api.TypeNameEdge, node.lastRequestTime)

	m.locatorManager.notifyNodeStatusToLocator(deviceID, false)
}

func (m *NodeManager) candidateOnline(node *CandidateNode) error {
	deviceID := node.deviceInfo.DeviceId

	// node.isValidator, _ = cache.GetDB().IsNodeInValidatorList(deviceID)

	// geo ip
	// geoInfo, _ := region.GetRegion().GetGeoInfo(node.deviceInfo.ExternalIp)
	// if err != nil {
	// 	log.Warnf("candidateOnline GetGeoInfo err:%v,ExternalIp:%v", err, node.deviceInfo.ExternalIp)
	// }
	isOk, geoInfo := ipLegality(node.deviceInfo.ExternalIp)
	if !isOk {
		log.Errorf("candidateOnline err DeviceId:%s,ip%s,geo:%s", deviceID, node.deviceInfo.ExternalIp, geoInfo.Geo)
		return xerrors.Errorf(ErrAreaNotExist, geoInfo.Geo, node.deviceInfo.ExternalIp)
	}

	node.geoInfo = geoInfo

	nodeOld := m.getCandidateNode(deviceID)
	if nodeOld != nil {
		nodeOld.closer()

		nodeOld = nil
	}

	err := node.setNodeOnline(api.TypeNameCandidate)
	if err != nil {
		// log.Errorf("addCandidateNode NodeOnline err:%v", err)
		return err
	}

	m.candidateNodeMap.Store(deviceID, node)
	// m.areaManager.addCandidate(node)

	m.validatePool.addPendingNode(nil, node)

	m.StateNetwork()

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

func (m *NodeManager) candidateOffline(node *CandidateNode) {
	deviceID := node.deviceInfo.DeviceId
	// close old node
	node.closer()

	log.Warnf("candidateOffline :%s", deviceID)

	m.candidateNodeMap.Delete(deviceID)
	// m.areaManager.removeCandidate(node)

	m.validatePool.removeCandidate(deviceID)

	node.setNodeOffline(deviceID, node.geoInfo, api.TypeNameCandidate, node.lastRequestTime)

	m.locatorManager.notifyNodeStatusToLocator(deviceID, false)
}

func (m *NodeManager) findEdgeNodes(useDeviceIDs []string, filterDeviceIDs map[string]string) []*EdgeNode {
	if filterDeviceIDs == nil {
		filterDeviceIDs = make(map[string]string)
	}

	// eMap := m.areaManager.getEdges(geoKey)
	// if eMap == nil || len(eMap) <= 0 {
	// 	return nil
	// }
	list := make([]*EdgeNode, 0)

	if useDeviceIDs != nil && len(useDeviceIDs) > 0 {
		for _, dID := range useDeviceIDs {
			if _, ok := filterDeviceIDs[dID]; ok {
				continue
			}

			node := m.getEdgeNode(dID)
			if node != nil {
				list = append(list, node)
			}
		}
	} else {
		m.edgeNodeMap.Range(func(key, value interface{}) bool {
			deviceID := key.(string)
			node := value.(*EdgeNode)

			if _, ok := filterDeviceIDs[deviceID]; ok {
				return true
			}

			if node == nil {
				return true
			}
			list = append(list, node)

			return true
		})

		// for _, node := range eMap {
		// 	if _, ok := filterDeviceIDs[node.deviceInfo.DeviceId]; ok {
		// 		continue
		// 	}

		// 	list = append(list, node)
		// }
	}

	if len(list) > 0 {
		sort.Slice(list, func(i, j int) bool {
			return list[i].deviceInfo.DeviceId > list[j].deviceInfo.DeviceId
		})

		return list
	}

	return nil
}

func (m *NodeManager) findCandidateNodes(useDeviceIDs []string, filterDeviceIDs map[string]string) []*CandidateNode {
	if filterDeviceIDs == nil {
		filterDeviceIDs = make(map[string]string)
	}

	// eMap := m.areaManager.getCandidates(geoKey)
	// if eMap == nil || len(eMap) <= 0 {
	// 	return nil
	// }
	list := make([]*CandidateNode, 0)

	if len(useDeviceIDs) > 0 {
		for _, dID := range useDeviceIDs {
			if _, ok := filterDeviceIDs[dID]; ok {
				continue
			}
			// node, ok := eMap[dID]
			node := m.getCandidateNode(dID)
			if node != nil {
				list = append(list, node)
			}
		}
	} else {
		m.candidateNodeMap.Range(func(key, value interface{}) bool {
			deviceID := key.(string)
			node := value.(*CandidateNode)

			if _, ok := filterDeviceIDs[deviceID]; ok {
				return true
			}

			if node == nil {
				return true
			}
			list = append(list, node)

			return true
		})
		// for _, node := range eMap {
		// 	if _, ok := filterDeviceIDs[node.deviceInfo.DeviceId]; ok {
		// 		continue
		// 	}
		// 	list = append(list, node)
		// }
	}

	if len(list) > 0 {
		sort.Slice(list, func(i, j int) bool {
			return list[i].deviceInfo.DeviceId > list[j].deviceInfo.DeviceId
		})
		return list
	}

	return nil
}

// func (m *NodeManager) findEdges(userGeoInfo *region.GeoInfo, useDeviceIDs []string, filterDeviceIDs map[string]string) ([]*EdgeNode, geoLevel) {
// 	if userGeoInfo != nil {
// 		countryKey, provinceKey, cityKey := m.areaManager.getAreaKey(userGeoInfo)

// 		list := m.findEdgeNodeWithGeo(cityKey, useDeviceIDs, filterDeviceIDs)
// 		if list != nil {
// 			return list, cityLevel
// 		}

// 		list = m.findEdgeNodeWithGeo(provinceKey, useDeviceIDs, filterDeviceIDs)
// 		if list != nil {
// 			return list, provinceLevel
// 		}

// 		list = m.findEdgeNodeWithGeo(countryKey, useDeviceIDs, filterDeviceIDs)
// 		if list != nil {
// 			return list, countryLevel
// 		}
// 	}

// 	list := m.findEdgeNodeWithGeo(defaultKey, useDeviceIDs, filterDeviceIDs)
// 	if list != nil {
// 		return list, defaultLevel
// 	}

// 	return nil, defaultLevel
// }

// func (m *NodeManager) findCandidates(userGeoInfo *region.GeoInfo, useDeviceIDs []string, filterDeviceIDs map[string]string) ([]*CandidateNode, geoLevel) {
// 	if userGeoInfo != nil {
// 		countryKey, provinceKey, cityKey := m.areaManager.getAreaKey(userGeoInfo)

// 		list := m.findCandidateNodeWithGeo(cityKey, useDeviceIDs, filterDeviceIDs)
// 		if list != nil {
// 			return list, cityLevel
// 		}

// 		list = m.findCandidateNodeWithGeo(provinceKey, useDeviceIDs, filterDeviceIDs)
// 		if list != nil {
// 			return list, provinceLevel
// 		}

// 		list = m.findCandidateNodeWithGeo(countryKey, useDeviceIDs, filterDeviceIDs)
// 		if list != nil {
// 			return list, countryLevel
// 		}
// 	}

// 	list := m.findCandidateNodeWithGeo(defaultKey, useDeviceIDs, filterDeviceIDs)
// 	if list != nil {
// 		return list, defaultLevel
// 	}

// 	return nil, defaultLevel
// }

// func (m *NodeManager) filterCandidates(filterDeviceIDs []string, nodes []*CandidateNode) []*CandidateNode {
// 	nodes2 := make([]*CandidateNode, 0)
// 	for _, node := range nodes {
// 		isHave := false
// 		for _, nd := range filterDeviceIDs {
// 			if node.deviceInfo.DeviceId == nd {
// 				isHave = true
// 			}
// 		}

// 		if !isHave {
// 			nodes2 = append(nodes2, node)
// 		}
// 	}

// 	return nodes2
// }

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
func (m *NodeManager) findNodeDownloadInfo(cid string) (api.DownloadInfo, error) {
	var downloadInfo api.DownloadInfo
	deviceIDs, err := persistent.GetDB().GetNodesWithCacheList(cid)
	if err != nil {
		return downloadInfo, err
	}

	if len(deviceIDs) <= 0 {
		return downloadInfo, xerrors.Errorf("%s , whit cid:%s", ErrNodeNotFind, cid)
	}

	nodeEs := m.findEdgeNodes(deviceIDs, nil)
	if nodeEs != nil {
		node := nodeEs[randomNum(0, len(nodeEs))]
		return node.nodeAPI.GetDownloadInfo(context.Background())
	}

	nodeCs := m.findCandidateNodes(deviceIDs, nil)
	if nodeCs != nil {
		node := nodeCs[randomNum(0, len(nodeCs))]
		return node.nodeAPI.GetDownloadInfo(context.Background())
	}

	return downloadInfo, xerrors.Errorf("%s , whit cid:%s", ErrNodeNotFind, cid)
}

// getCandidateNodesWithData find device
func (m *NodeManager) getCandidateNodesWithData(cid string) ([]*CandidateNode, error) {
	deviceIDs, err := persistent.GetDB().GetNodesWithCacheList(cid)
	if err != nil {
		return nil, err
	}
	// log.Infof("getCandidateNodesWithData deviceIDs : %v", deviceIDs)

	if len(deviceIDs) <= 0 {
		return nil, xerrors.Errorf("%s , whit cid:%s", ErrNodeNotFind, cid)
	}

	nodeCs := m.findCandidateNodes(deviceIDs, nil)

	return nodeCs, nil
}

// AreaManager Node Area Manager
// type AreaManager struct {
// 	edgeNodeMap      sync.Map
// 	candidateNodeMap sync.Map
// }

// func (a *AreaManager) getAllAreaMap() (map[string]map[string]*EdgeNode, map[string]map[string]*CandidateNode) {
// 	edges := make(map[string]map[string]*EdgeNode)
// 	candidates := make(map[string]map[string]*CandidateNode)

// 	a.edgeNodeMap.Range(func(key, value interface{}) bool {
// 		geo := key.(string)

// 		if len(strings.Split(geo, "-")) < 3 {
// 			// not city
// 			return true
// 		}
// 		// city
// 		m := value.(map[string]*EdgeNode)
// 		edges[geo] = m

// 		return true
// 	})

// 	a.candidateNodeMap.Range(func(key, value interface{}) bool {
// 		geo := key.(string)

// 		if len(strings.Split(geo, "-")) < 3 {
// 			// not city
// 			return true
// 		}
// 		// city
// 		m := value.(map[string]*CandidateNode)
// 		candidates[geo] = m

// 		return true
// 	})

// 	return edges, candidates
// }

// func (a *AreaManager) getAreaKey(geoInfo *region.GeoInfo) (countryKey, provinceKey, cityKey string) {
// 	if geoInfo == nil {
// 		geoInfo = region.GetRegion().DefaultGeoInfo("")
// 	}
// 	countryKey = geoInfo.Country
// 	provinceKey = fmt.Sprintf("%s-%s", geoInfo.Country, geoInfo.Province)
// 	cityKey = fmt.Sprintf("%s-%s-%s", geoInfo.Country, geoInfo.Province, geoInfo.City)

// 	return
// }

// func (a *AreaManager) addEdge(node *EdgeNode) {
// 	countryKey, provinceKey, cityKey := a.getAreaKey(node.geoInfo)

// 	a.storeEdge(node, countryKey)
// 	a.storeEdge(node, provinceKey)
// 	a.storeEdge(node, cityKey)
// 	a.storeEdge(node, defaultKey)
// }

// func (a *AreaManager) removeEdge(node *EdgeNode) {
// 	countryKey, provinceKey, cityKey := a.getAreaKey(node.geoInfo)

// 	deviceID := node.deviceInfo.DeviceId

// 	a.deleteEdge(deviceID, countryKey)
// 	a.deleteEdge(deviceID, provinceKey)
// 	a.deleteEdge(deviceID, cityKey)
// 	a.deleteEdge(deviceID, defaultKey)
// }

// func (a *AreaManager) getEdges(key string) map[string]*EdgeNode {
// 	m, ok := a.edgeNodeMap.Load(key)
// 	if ok && m != nil {
// 		return m.(map[string]*EdgeNode)
// 	}

// 	return nil
// }

// func (a *AreaManager) addCandidate(node *CandidateNode) {
// 	countryKey, provinceKey, cityKey := a.getAreaKey(node.geoInfo)

// 	a.storeCandidate(node, countryKey)
// 	a.storeCandidate(node, provinceKey)
// 	a.storeCandidate(node, cityKey)
// 	a.storeCandidate(node, defaultKey)
// }

// func (a *AreaManager) removeCandidate(node *CandidateNode) {
// 	countryKey, provinceKey, cityKey := a.getAreaKey(node.geoInfo)

// 	deviceID := node.deviceInfo.DeviceId

// 	a.deleteCandidate(deviceID, countryKey)
// 	a.deleteCandidate(deviceID, provinceKey)
// 	a.deleteCandidate(deviceID, cityKey)
// 	a.deleteCandidate(deviceID, defaultKey)
// }

// func (a *AreaManager) getCandidates(key string) map[string]*CandidateNode {
// 	m, ok := a.candidateNodeMap.Load(key)
// 	if ok && m != nil {
// 		return m.(map[string]*CandidateNode)
// 	}

// 	return nil
// }

// func (a *AreaManager) storeEdge(node *EdgeNode, key string) {
// 	countryMap := make(map[string]*EdgeNode)
// 	m, ok := a.edgeNodeMap.Load(key)
// 	if ok && m != nil {
// 		countryMap = m.(map[string]*EdgeNode)
// 	}

// 	countryMap[node.deviceInfo.DeviceId] = node
// 	a.edgeNodeMap.Store(key, countryMap)
// }

// func (a *AreaManager) deleteEdge(deviceID string, key string) {
// 	m, ok := a.edgeNodeMap.Load(key)
// 	if ok && m != nil {
// 		nodeMap := m.(map[string]*EdgeNode)
// 		_, ok := nodeMap[deviceID]
// 		if ok {
// 			delete(nodeMap, deviceID)
// 			a.edgeNodeMap.Store(key, nodeMap)
// 		}
// 	}
// }

// func (a *AreaManager) storeCandidate(node *CandidateNode, key string) {
// 	countryMap := make(map[string]*CandidateNode)
// 	m, ok := a.candidateNodeMap.Load(key)
// 	if ok && m != nil {
// 		countryMap = m.(map[string]*CandidateNode)
// 	}

// 	countryMap[node.deviceInfo.DeviceId] = node
// 	a.candidateNodeMap.Store(key, countryMap)
// }

// func (a *AreaManager) deleteCandidate(deviceID string, key string) {
// 	m, ok := a.candidateNodeMap.Load(key)
// 	if ok && m != nil {
// 		nodeMap := m.(map[string]*CandidateNode)
// 		_, ok := nodeMap[deviceID]
// 		if ok {
// 			delete(nodeMap, deviceID)
// 			a.candidateNodeMap.Store(key, nodeMap)
// 		}
// 	}
// }

func (n *NodeManager) SetDeviceInfo(deviceID string, info api.DevicesInfo) error {
	_, err := cache.GetDB().SetDeviceInfo(deviceID, info)
	if err != nil {
		log.Errorf("set device info: %s", err.Error())
		return err
	}
	return nil
}

func (n *NodeManager) StateNetwork() error {
	state, err := cache.GetDB().GetDeviceStat()
	if err != nil {
		log.Errorf("get node stat: %v", err)
		return err
	}

	state.AllVerifier = len(n.validatePool.veriftorList)
	n.state = state
	return nil
}

func (n *NodeManager) isDeviceExist(deviceID string, nodeType int) bool {
	info, err := persistent.GetDB().GetRegisterInfo(deviceID)
	if err != nil {
		return false
	}

	if nodeType != 0 {
		return info.NodeType == nodeType
	}

	return true
}
