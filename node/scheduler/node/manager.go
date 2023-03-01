package node

import (
	"context"
	"sync"
	"time"

	logging "github.com/ipfs/go-log/v2"
	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/node/cidutil"
	"github.com/linguohua/titan/node/scheduler/db/persistent"
	"github.com/linguohua/titan/node/scheduler/locator"
	"golang.org/x/xerrors"
)

var log = logging.Logger("node")

const (
	nodeOfflineTime = 24 // If it is not online after this time, it is determined that the node has quit the system (unit: hour)

	keepaliveTime    = 30 // keepalive time interval (unit: second)
	saveInfoInterval = 10 // keepalive saves information every 10 times
)

// Manager Node Manager
type Manager struct {
	EdgeNodes      sync.Map
	CandidateNodes sync.Map

	nodeQuitCallBack func([]string)
}

// NewManager New
func NewManager(callBack func([]string)) *Manager {
	nodeManager := &Manager{
		nodeQuitCallBack: callBack,
	}

	go nodeManager.run()

	return nodeManager
}

func (m *Manager) run() {
	ticker := time.NewTicker(time.Duration(keepaliveTime) * time.Second)
	defer ticker.Stop()

	count := 0

	for {
		select {
		case <-ticker.C:
			count++
			isSave := count%saveInfoInterval == 0

			m.checkNodesKeepalive(isSave)

			// check node offline how time
			m.checkWhetherNodeQuits()
		}
	}
}

func (m *Manager) edgeKeepalive(node *Edge, nowTime time.Time, isSave bool) {
	lastTime := node.LastRequestTime()

	if !lastTime.After(nowTime) {
		// offline
		m.edgeOffline(node)
		node = nil
		return
	}

	if isSave {
		err := persistent.UpdateNodeOnlineTime(node.DeviceID, saveInfoInterval*keepaliveTime)
		if err != nil {
			log.Errorf("UpdateNodeOnlineTime err:%s,deviceID:%s", err.Error(), node.DeviceID)
		}
	}
}

func (m *Manager) candidateKeepalive(node *Candidate, nowTime time.Time, isSave bool) {
	lastTime := node.LastRequestTime()

	if !lastTime.After(nowTime) {
		// offline
		m.candidateOffline(node)
		node = nil
		return
	}

	if isSave {
		err := persistent.UpdateNodeOnlineTime(node.DeviceID, saveInfoInterval*keepaliveTime)
		if err != nil {
			log.Errorf("UpdateNodeOnlineTime err:%s,deviceID:%s", err.Error(), node.DeviceID)
		}
	}
}

func (m *Manager) checkNodesKeepalive(isSave bool) {
	nowTime := time.Now().Add(-time.Duration(keepaliveTime) * time.Second)

	m.EdgeNodes.Range(func(key, value interface{}) bool {
		// deviceID := key.(string)
		node := value.(*Edge)

		if node == nil {
			return true
		}

		go m.edgeKeepalive(node, nowTime, isSave)

		return true
	})

	m.CandidateNodes.Range(func(key, value interface{}) bool {
		// deviceID := key.(string)
		node := value.(*Candidate)

		if node == nil {
			return true
		}

		go m.candidateKeepalive(node, nowTime, isSave)

		return true
	})
}

// GetOnlineNodes get nodes with type
func (m *Manager) GetOnlineNodes(nodeType api.NodeType) ([]string, error) {
	list := make([]string, 0)

	if nodeType == api.NodeUnknown || nodeType == api.NodeCandidate {
		m.CandidateNodes.Range(func(key, value interface{}) bool {
			deviceID := key.(string)
			list = append(list, deviceID)

			return true
		})
	}

	if nodeType == api.NodeUnknown || nodeType == api.NodeEdge {
		m.EdgeNodes.Range(func(key, value interface{}) bool {
			deviceID := key.(string)
			list = append(list, deviceID)

			return true
		})
	}

	return list, nil
}

// EdgeOnline Edge Online
func (m *Manager) EdgeOnline(node *Edge) error {
	deviceID := node.DeviceID

	nodeOld := m.GetEdgeNode(deviceID)
	if nodeOld != nil {
		nodeOld.ClientCloser()

		nodeOld = nil
	}

	err := node.saveInfo()
	if err != nil {
		return err
	}

	m.EdgeNodes.Store(deviceID, node)

	return nil
}

// GetEdgeNode Get EdgeNode
func (m *Manager) GetEdgeNode(deviceID string) *Edge {
	nodeI, exist := m.EdgeNodes.Load(deviceID)
	if exist && nodeI != nil {
		node := nodeI.(*Edge)

		return node
	}

	return nil
}

func (m *Manager) edgeOffline(node *Edge) {
	deviceID := node.DeviceID
	// close old node
	node.ClientCloser()

	log.Infof("Edge Offline :%s", deviceID)

	m.EdgeNodes.Delete(deviceID)

	// notify locator
	locator.ChangeNodeOnlineStatus(deviceID, false)
}

// CandidateOnline Candidate Online
func (m *Manager) CandidateOnline(node *Candidate) error {
	deviceID := node.DeviceID

	nodeOld := m.GetCandidateNode(deviceID)
	if nodeOld != nil {
		nodeOld.ClientCloser()

		nodeOld = nil
	}

	err := node.saveInfo()
	if err != nil {
		return err
	}

	m.CandidateNodes.Store(deviceID, node)

	return nil
}

// GetCandidateNode Get Candidate Node
func (m *Manager) GetCandidateNode(deviceID string) *Candidate {
	nodeI, exist := m.CandidateNodes.Load(deviceID)
	if exist && nodeI != nil {
		node := nodeI.(*Candidate)

		return node
	}

	return nil
}

func (m *Manager) candidateOffline(node *Candidate) {
	deviceID := node.DeviceID
	// close old node
	node.ClientCloser()

	log.Infof("Candidate Offline :%s", deviceID)

	m.CandidateNodes.Delete(deviceID)

	// notify locator
	locator.ChangeNodeOnlineStatus(deviceID, false)
}

// FindCandidates Find CandidateNodes from all Candidate and filter filterMap
func (m *Manager) FindCandidates(filterMap map[string]string) []*Candidate {
	if filterMap == nil {
		filterMap = make(map[string]string)
	}

	nodes := make([]*Candidate, 0)

	m.CandidateNodes.Range(func(key, value interface{}) bool {
		deviceID := key.(string)
		node := value.(*Candidate)

		if _, exist := filterMap[deviceID]; exist {
			return true
		}

		if node != nil {
			nodes = append(nodes, node)
		}

		return true
	})

	if len(nodes) > 0 {
		return nodes
	}

	return nil
}

// FindCandidatesByList Find CandidateNodes by list and filter filterMap
func (m *Manager) FindCandidatesByList(list []string, filterMap map[string]string) []*Candidate {
	if list == nil {
		return nil
	}

	if filterMap == nil {
		filterMap = make(map[string]string)
	}

	nodes := make([]*Candidate, 0)

	for _, dID := range list {
		if _, exist := filterMap[dID]; exist {
			continue
		}

		node := m.GetCandidateNode(dID)
		if node != nil {
			nodes = append(nodes, node)
		}
	}

	if len(nodes) > 0 {
		return nodes
	}

	return nil
}

// NodeSessionCallBack node pingpong
func (m *Manager) NodeSessionCallBack(deviceID, remoteAddr string) {
	lastTime := time.Now()

	edge := m.GetEdgeNode(deviceID)
	if edge != nil {
		edge.SetLastRequestTime(lastTime)
		edge.ConnectRPC(remoteAddr, false)
		return
	}

	candidate := m.GetCandidateNode(deviceID)
	if candidate != nil {
		candidate.SetLastRequestTime(lastTime)
		candidate.ConnectRPC(remoteAddr, false)
		return
	}
}

// FindNodeDownloadInfos  find device with block cid
func (m *Manager) FindNodeDownloadInfos(cid, userURL string) ([]*api.DownloadInfoResult, error) {
	infos := make([]*api.DownloadInfoResult, 0)

	hash, err := cidutil.CIDString2HashString(cid)
	if err != nil {
		return nil, xerrors.Errorf("%s cid to hash err:%s", cid, err.Error())
	}

	caches, err := persistent.CarfileReplicaInfosWithHash(hash, true)
	if err != nil {
		return nil, err
	}

	if len(caches) <= 0 {
		return nil, xerrors.Errorf("not found node , with hash:%s", hash)
	}

	for _, cache := range caches {
		deviceID := cache.DeviceID
		node := m.GetEdgeNode(deviceID)
		if node == nil {
			continue
		}

		url := node.DownloadURL()

		err := node.nodeAPI.PingUser(context.Background(), userURL)
		if err != nil {
			log.Errorf("%s PingUser err:%s", deviceID, err.Error())
			continue
		}

		infos = append(infos, &api.DownloadInfoResult{URL: url, DeviceID: deviceID})
	}

	return infos, nil
}

// GetCandidatesWithBlockHash find candidates with block hash
func (m *Manager) GetCandidatesWithBlockHash(hash, filterDevice string) ([]*Candidate, error) {
	caches, err := persistent.CarfileReplicaInfosWithHash(hash, true)
	if err != nil {
		return nil, err
	}

	if len(caches) <= 0 {
		return nil, xerrors.Errorf("not found node, with hash:%s", hash)
	}

	nodes := make([]*Candidate, 0)

	for _, cache := range caches {
		dID := cache.DeviceID
		if dID == filterDevice {
			continue
		}

		node := m.GetCandidateNode(dID)
		if node != nil {
			nodes = append(nodes, node)
		}
	}

	return nodes, nil
}

func (m *Manager) checkWhetherNodeQuits() {
	nodes, err := persistent.LongTimeOfflineNodes(nodeOfflineTime)
	if err != nil {
		log.Errorf("checkWhetherNodeQuits GetOfflineNodes err:%s", err.Error())
		return
	}

	t := time.Now().Add(-time.Duration(nodeOfflineTime) * time.Hour)

	quits := make([]string, 0)

	for _, node := range nodes {
		if node.LastTime.After(t) {
			continue
		}

		// node quitted
		quits = append(quits, node.DeviceID)
	}

	if len(quits) > 0 {
		m.NodesQuit(quits)
	}
}

// NodesQuit Nodes quit
func (m *Manager) NodesQuit(deviceIDs []string) {
	err := persistent.SetNodesQuit(deviceIDs)
	if err != nil {
		log.Errorf("NodeExited SetNodesQuit err:%s", err.Error())
		return
	}

	if m.nodeQuitCallBack != nil {
		m.nodeQuitCallBack(deviceIDs)
	}
}

// GetNode get node
func (m *Manager) GetNode(deviceID string) *BaseInfo {
	edge := m.GetEdgeNode(deviceID)
	if edge != nil {
		return edge.BaseInfo
	}

	candidate := m.GetCandidateNode(deviceID)
	if candidate != nil {
		return candidate.BaseInfo
	}

	return nil
}
