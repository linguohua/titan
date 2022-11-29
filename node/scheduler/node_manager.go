package scheduler

import (
	"crypto/rsa"
	"fmt"
	"sync"
	"time"

	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/node/helper"
	"github.com/linguohua/titan/node/scheduler/db/cache"
	"github.com/linguohua/titan/node/scheduler/db/persistent"
	"github.com/ouqiang/timewheel"
	"golang.org/x/xerrors"
)

// node manager

// type geoLevel int64

const (
// defaultLevel  geoLevel = 0
// countryLevel  geoLevel = 1
// provinceLevel geoLevel = 2
// cityLevel     geoLevel = 3

// defaultKey = "default"
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

	m.stateNetwork()

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

	m.stateNetwork()

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

func (m *NodeManager) findEdgeNodes(useDeviceIDs []string, skips map[string]string) []*EdgeNode {
	if skips == nil {
		skips = make(map[string]string)
	}

	list := make([]*EdgeNode, 0)

	if useDeviceIDs != nil && len(useDeviceIDs) > 0 {
		for _, dID := range useDeviceIDs {
			if _, ok := skips[dID]; ok {
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

			if _, ok := skips[deviceID]; ok {
				return true
			}

			if node == nil {
				return true
			}
			list = append(list, node)

			return true
		})
	}

	if len(list) > 0 {
		return list
	}

	return nil
}

func (m *NodeManager) findCandidateNodes(useDeviceIDs []string, skips map[string]string) []*CandidateNode {
	if skips == nil {
		skips = make(map[string]string)
	}

	list := make([]*CandidateNode, 0)

	if len(useDeviceIDs) > 0 {
		for _, dID := range useDeviceIDs {
			if _, ok := skips[dID]; ok {
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

			if _, ok := skips[deviceID]; ok {
				return true
			}

			if node == nil {
				return true
			}
			list = append(list, node)

			return true
		})
	}

	if len(list) > 0 {
		return list
	}

	return nil
}

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
func (m *NodeManager) findNodeDownloadInfos(cid string) ([]api.DownloadInfoResult, error) {
	infos := make([]api.DownloadInfoResult, 0)

	hash, err := helper.CIDString2HashString(cid)
	if err != nil {
		return nil, xerrors.Errorf("%s cid to hash err:%s", cid, err.Error())
	}

	deviceIDs, err := persistent.GetDB().GetNodesWithCache(hash, true)
	if err != nil {
		return nil, err
	}

	if len(deviceIDs) <= 0 {
		return nil, xerrors.Errorf("%s , with hash:%s", ErrNodeNotFind, hash)
	}

	for _, deviceID := range deviceIDs {
		info, err := persistent.GetDB().GetNodeAuthInfo(deviceID)
		if err != nil {
			continue
		}

		privateKey, err := m.getDeviccePrivateKey(deviceID, info)
		if err != nil {
			continue
		}

		sn, err := cache.GetDB().IncrBlockDownloadSN()
		if err != nil {
			continue
		}

		signTime := time.Now().Unix()
		sign, err := rsaSign(privateKey, fmt.Sprintf("%s%d%d%d", cid, sn, signTime, blockDonwloadTimeout))
		if err != nil {
			continue
		}

		infos = append(infos, api.DownloadInfoResult{URL: info.URL, Sign: sign, SN: sn, SignTime: time.Now().Unix(), TimeOut: blockDonwloadTimeout})
	}

	if len(infos) <= 0 {
		return nil, xerrors.Errorf("device auth err, deviceIDs:%v", deviceIDs)
	}

	return infos, nil
}

// getCandidateNodesWithData find device
func (m *NodeManager) getCandidateNodesWithData(hash, skip string) ([]*CandidateNode, error) {
	deviceIDs, err := persistent.GetDB().GetNodesWithCache(hash, true)
	if err != nil {
		return nil, err
	}
	// log.Infof("getCandidateNodesWithData deviceIDs : %v", deviceIDs)

	if len(deviceIDs) <= 0 {
		return nil, xerrors.Errorf("%s , with hash:%s", ErrNodeNotFind, hash)
	}

	skips := make(map[string]string)

	if skip != "" {
		skips[skip] = hash
	}

	nodeCs := m.findCandidateNodes(deviceIDs, skips)

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

func (m *NodeManager) setDeviceInfo(deviceID string, info api.DevicesInfo) error {
	_, err := cache.GetDB().SetDeviceInfo(deviceID, info)
	if err != nil {
		log.Errorf("set device info: %s", err.Error())
		return err
	}
	return nil
}

func (m *NodeManager) stateNetwork() error {
	state, err := cache.GetDB().GetDeviceStat()
	if err != nil {
		log.Errorf("get node stat: %v", err)
		return err
	}

	state.AllVerifier = len(m.validatePool.veriftorList)
	m.state = state
	return nil
}

func (m *NodeManager) isDeviceExist(deviceID string, nodeType int) bool {
	info, err := persistent.GetDB().GetRegisterInfo(deviceID)
	if err != nil {
		return false
	}

	if nodeType != 0 {
		return info.NodeType == nodeType
	}

	return true
}

func (m *NodeManager) getDeviccePrivateKey(deviceID string, authInfo *api.DownloadServerAccessAuth) (*rsa.PrivateKey, error) {
	edge := m.getEdgeNode(deviceID)
	if edge != nil {
		return edge.privateKey, nil
	}

	candidate := m.getCandidateNode(deviceID)
	if candidate != nil {
		return candidate.privateKey, nil
	}

	privateKey, err := pem2PrivateKey(authInfo.PrivateKey)
	if err != nil {
		return nil, err
	}

	return privateKey, nil
}
