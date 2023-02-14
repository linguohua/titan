package node

import (
	"sync"
	"time"

	logging "github.com/ipfs/go-log/v2"
	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/node/cidutil"
	"github.com/linguohua/titan/node/scheduler/db/cache"
	"github.com/linguohua/titan/node/scheduler/db/persistent"
	"golang.org/x/xerrors"
)

var log = logging.Logger("node")

const (
	nodeExitTime = 24 // If it is not online after this time, it is determined that the node has quit the system (hour)

	keepaliveTime = 30 // keepalive time interval (second)
)

// Manager Node Manager
type Manager struct {
	EdgeNodeMap      sync.Map
	CandidateNodeMap sync.Map

	nodeOfflineCallBack func(string)
	nodeExitedCallBack  func([]string)
}

// NewNodeManager New
func NewNodeManager(nodeOfflineCallBack func(string), nodeExitedCallBack func([]string)) *Manager {
	nodeManager := &Manager{
		nodeOfflineCallBack: nodeOfflineCallBack,
		nodeExitedCallBack:  nodeExitedCallBack,
	}

	go nodeManager.run()

	return nodeManager
}

func (m *Manager) run() {
	ticker := time.NewTicker(time.Duration(keepaliveTime) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			m.checkNodesKeepalive()

			// check node offline how time
			m.checkNodesQuit()
		}
	}
}

func (m *Manager) edgeKeepalive(node *EdgeNode, nowTime time.Time) {
	lastTime := node.GetLastRequestTime()

	if !lastTime.After(nowTime) {
		// offline
		m.edgeOffline(node)
		node = nil
		return
	}

	_, err := cache.GetDB().IncrNodeOnlineTime(node.DeviceID, keepaliveTime)
	if err != nil {
		log.Errorf("IncrNodeOnlineTime err:%s,deviceID:%s", err.Error(), node.DeviceID)
	}
}

func (m *Manager) candidateKeepalive(node *CandidateNode, nowTime time.Time) {
	lastTime := node.GetLastRequestTime()

	if !lastTime.After(nowTime) {
		// offline
		m.candidateOffline(node)
		node = nil
		return
	}

	_, err := cache.GetDB().IncrNodeOnlineTime(node.DeviceID, keepaliveTime)
	if err != nil {
		log.Errorf("IncrNodeOnlineTime err:%s,deviceID:%s", err.Error(), node.DeviceID)
	}
}

func (m *Manager) checkNodesKeepalive() {
	nowTime := time.Now().Add(-time.Duration(keepaliveTime) * time.Second)

	m.EdgeNodeMap.Range(func(key, value interface{}) bool {
		// deviceID := key.(string)
		node := value.(*EdgeNode)

		if node == nil {
			return true
		}

		go m.edgeKeepalive(node, nowTime)

		return true
	})

	m.CandidateNodeMap.Range(func(key, value interface{}) bool {
		// deviceID := key.(string)
		node := value.(*CandidateNode)

		if node == nil {
			return true
		}

		go m.candidateKeepalive(node, nowTime)

		return true
	})
}

// GetOnlineNodes get nodes with type
func (m *Manager) GetOnlineNodes(nodeType api.NodeTypeName) ([]string, error) {
	list := make([]string, 0)

	if nodeType == api.TypeNameAll || nodeType == api.TypeNameCandidate {
		m.CandidateNodeMap.Range(func(key, value interface{}) bool {
			deviceID := key.(string)
			list = append(list, deviceID)

			return true
		})
	}

	if nodeType == api.TypeNameAll || nodeType == api.TypeNameEdge {
		m.EdgeNodeMap.Range(func(key, value interface{}) bool {
			deviceID := key.(string)
			list = append(list, deviceID)

			return true
		})
	}

	return list, nil
}

// EdgeOnline Edge Online
func (m *Manager) EdgeOnline(node *EdgeNode) error {
	deviceID := node.DeviceID

	nodeOld := m.GetEdgeNode(deviceID)
	if nodeOld != nil {
		nodeOld.ClientCloser()

		nodeOld = nil
	}

	err := node.setNodeOnline()
	if err != nil {
		return err
	}

	m.EdgeNodeMap.Store(deviceID, node)

	return nil
}

// GetEdgeNode Get EdgeNode
func (m *Manager) GetEdgeNode(deviceID string) *EdgeNode {
	nodeI, exist := m.EdgeNodeMap.Load(deviceID)
	if exist && nodeI != nil {
		node := nodeI.(*EdgeNode)

		return node
	}

	return nil
}

func (m *Manager) edgeOffline(node *EdgeNode) {
	deviceID := node.DeviceID
	// close old node
	node.ClientCloser()

	log.Infof("edgeOffline :%s", deviceID)

	m.EdgeNodeMap.Delete(deviceID)

	node.setNodeOffline()

	if m.nodeOfflineCallBack != nil {
		m.nodeOfflineCallBack(deviceID)
	}
}

// CandidateOnline Candidate Online
func (m *Manager) CandidateOnline(node *CandidateNode) error {
	deviceID := node.DeviceID

	nodeOld := m.GetCandidateNode(deviceID)
	if nodeOld != nil {
		nodeOld.ClientCloser()

		nodeOld = nil
	}

	err := node.setNodeOnline()
	if err != nil {
		// log.Errorf("addCandidateNode NodeOnline err:%v", err)
		return err
	}

	m.CandidateNodeMap.Store(deviceID, node)

	return nil
}

// GetCandidateNode Get Candidate Node
func (m *Manager) GetCandidateNode(deviceID string) *CandidateNode {
	nodeI, exist := m.CandidateNodeMap.Load(deviceID)
	if exist && nodeI != nil {
		node := nodeI.(*CandidateNode)

		return node
	}

	return nil
}

func (m *Manager) candidateOffline(node *CandidateNode) {
	deviceID := node.DeviceID
	// close old node
	node.ClientCloser()

	log.Infof("candidateOffline :%s", deviceID)

	m.CandidateNodeMap.Delete(deviceID)

	node.setNodeOffline()

	if m.nodeOfflineCallBack != nil {
		m.nodeOfflineCallBack(deviceID)
	}
}

// FindCandidates Find CandidateNodes from all Candidate and filter filterMap
func (m *Manager) FindCandidates(filterMap map[string]string) []*CandidateNode {
	if filterMap == nil {
		filterMap = make(map[string]string)
	}

	nodes := make([]*CandidateNode, 0)

	m.CandidateNodeMap.Range(func(key, value interface{}) bool {
		deviceID := key.(string)
		node := value.(*CandidateNode)

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
func (m *Manager) FindCandidatesByList(list []string, filterMap map[string]string) []*CandidateNode {
	if list == nil {
		return nil
	}

	if filterMap == nil {
		filterMap = make(map[string]string)
	}

	nodes := make([]*CandidateNode, 0)

	for _, dID := range list {
		if _, exist := filterMap[dID]; exist {
			continue
		}
		// node, ok := eMap[dID]
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

// UpdateLastRequestTime node pingpong
func (m *Manager) UpdateLastRequestTime(deviceID string) {
	lastTime := time.Now()

	edge := m.GetEdgeNode(deviceID)
	if edge != nil {
		edge.SetLastRequestTime(lastTime)
		return
	}

	candidate := m.GetCandidateNode(deviceID)
	if candidate != nil {
		candidate.SetLastRequestTime(lastTime)
		return
	}
}

// FindNodeDownloadInfos  find device with block cid
func (m *Manager) FindNodeDownloadInfos(cid string) ([]*api.DownloadInfoResult, error) {
	infos := make([]*api.DownloadInfoResult, 0)

	hash, err := cidutil.CIDString2HashString(cid)
	if err != nil {
		return nil, xerrors.Errorf("%s cid to hash err:%s", cid, err.Error())
	}

	caches, err := persistent.GetDB().GetCacheTaskInfosWithHash(hash, true)
	if err != nil {
		return nil, err
	}

	if len(caches) <= 0 {
		return nil, xerrors.Errorf("not found node , with hash:%s", hash)
	}

	for _, cache := range caches {
		if cache.IsCandidate {
			continue
		}
		deviceID := cache.DeviceID

		info, err := persistent.GetDB().GetNodeAuthInfo(deviceID)
		if err != nil {
			continue
		}

		infos = append(infos, &api.DownloadInfoResult{URL: info.URL, DeviceID: deviceID})
	}

	// if len(infos) <= 0 {
	// 	return nil, xerrors.Errorf("device auth err, deviceIDs:%v", deviceIDs)
	// }

	return infos, nil
}

// GetCandidatesWithBlockHash find candidates with block hash
func (m *Manager) GetCandidatesWithBlockHash(hash, filterDevice string) ([]*CandidateNode, error) {
	caches, err := persistent.GetDB().GetCacheTaskInfosWithHash(hash, true)
	if err != nil {
		return nil, err
	}

	if len(caches) <= 0 {
		return nil, xerrors.Errorf("not found node, with hash:%s", hash)
	}

	nodes := make([]*CandidateNode, 0)

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

func (m *Manager) checkNodesQuit() {
	nodes, err := persistent.GetDB().GetOfflineNodes()
	if err != nil {
		log.Errorf("checkNodesQuit GetOfflineNodes err:%s", err.Error())
		return
	}

	t := time.Now().Add(-time.Duration(nodeExitTime) * time.Hour)

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
	err := persistent.GetDB().SetNodesQuit(deviceIDs)
	if err != nil {
		log.Errorf("NodeExited SetNodesQuit err:%s", err.Error())
		return
	}

	if m.nodeExitedCallBack != nil {
		m.nodeExitedCallBack(deviceIDs)
	}
}

// GetNode get node
func (m *Manager) GetNode(deviceID string) *Node {
	edge := m.GetEdgeNode(deviceID)
	if edge != nil {
		return edge.Node
	}

	candidate := m.GetCandidateNode(deviceID)
	if candidate != nil {
		return candidate.Node
	}

	return nil
}

// func statisticsPing(ip string) (*ping.Statistics, error) {
// 	pinger, err := ping.NewPinger(ip)
// 	if err != nil {
// 		return nil, err
// 	}

// 	if runtime.GOOS == "windows" {
// 		pinger.SetPrivileged(true)
// 	}

// 	pinger.Count = 3
// 	pinger.Timeout = 5 * time.Second
// 	err = pinger.Run()
// 	if err != nil {
// 		return nil, err
// 	}

// 	return pinger.Statistics(), nil
// }
