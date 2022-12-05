package scheduler

import (
	"crypto/rsa"
	"sync"
	"time"

	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/node/helper"
	"github.com/linguohua/titan/node/scheduler/db/cache"
	"github.com/linguohua/titan/node/scheduler/db/persistent"
	"golang.org/x/xerrors"
)

const exitTime = 5 // If it is not online after this time, it is determined that the node has exited the system (hour)

// NodeManager Node Manager
type NodeManager struct {
	edgeNodeMap      sync.Map
	candidateNodeMap sync.Map

	keepaliveTime float64 // keepalive time interval (minute)

	validatePool   *ValidatePool
	locatorManager *LocatorManager

	scheduler *Scheduler
}

func newNodeManager(pool *ValidatePool, locatorManager *LocatorManager, scheduler *Scheduler) *NodeManager {
	nodeManager := &NodeManager{
		keepaliveTime:  0.5,
		validatePool:   pool,
		locatorManager: locatorManager,
		scheduler:      scheduler,
	}

	go nodeManager.run()

	return nodeManager
}

func (m *NodeManager) run() {
	ticker := time.NewTicker(time.Duration(m.keepaliveTime*60) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			m.nodeKeepalive()

			// check node offline how time
			m.checkNodeExited()
		}
	}
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

		infos = append(infos, api.DownloadInfoResult{URL: info.URL, DeviceID: deviceID})
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

func (m *NodeManager) setDeviceInfo(deviceID string, info api.DevicesInfo) error {
	_, err := cache.GetDB().SetDeviceInfo(deviceID, info)
	if err != nil {
		log.Errorf("set device info: %s", err.Error())
		return err
	}
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

func (m *NodeManager) checkNodeExited() {
	nodes, err := persistent.GetDB().GetOfflineNodes()
	if err != nil {
		log.Warnf("checkNodeExited GetOfflineNodes err:%s", err.Error())
		return
	}

	t := time.Now().Add(-time.Duration(exitTime) * time.Hour)

	exiteds := make([]string, 0)

	for _, node := range nodes {
		if node.LastTime.After(t) {
			continue
		}

		// node exited
		exiteds = append(exiteds, node.DeviceID)

		// clean node cache
		m.nodeExited(node.DeviceID)
	}
}

func (m *NodeManager) nodeExited(deviceID string) {
	err := persistent.GetDB().SetNodeExited(deviceID)
	if err != nil {
		log.Warnf("checkNodeExited SetNodeExited err:%s", err.Error())
		return
	}

	// clean node cache
	m.scheduler.dataManager.cleanNodeAndRestoreCaches(deviceID)
}
