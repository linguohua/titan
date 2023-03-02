package node

import (
	"context"
	"github.com/linguohua/titan/node/common"
	"github.com/linguohua/titan/node/scheduler/db/cache"
	"sync"
	"time"

	logging "github.com/ipfs/go-log/v2"
	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/node/cidutil"
	titanRsa "github.com/linguohua/titan/node/rsa"
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

// ExitCallbackFunc will be invoked when a node exit
type ExitCallbackFunc func([]string)

// NewExitCallbackFunc ...
func NewExitCallbackFunc(cdb *persistent.CarfileDB) (ExitCallbackFunc, error) {
	return func(deviceIDs []string) {
		log.Infof("node event , nodes quit:%v", deviceIDs)

		hashs, err := cdb.LoadCarfileRecordsWithNodes(deviceIDs)
		if err != nil {
			log.Errorf("LoadCarfileRecordsWithNodes err:%s", err.Error())
			return
		}

		err = cdb.RemoveReplicaInfoWithNodes(deviceIDs)
		if err != nil {
			log.Errorf("RemoveReplicaInfoWithNodes err:%s", err.Error())
			return
		}

		// recache
		for _, hash := range hashs {
			log.Infof("need restore carfile :%s", hash)
		}
	}, nil
}

// Manager Node Manager
type Manager struct {
	EdgeNodes      sync.Map
	CandidateNodes sync.Map
	CarfileCache   *cache.CarfileCache
	NodeMgrCache   *cache.NodeMgrCache
	CarfileDB      *persistent.CarfileDB
	NodeMgrDB      *persistent.NodeMgrDB

	nodeQuitCallBack func([]string)
}

// NewManager return new node manager instance
func NewManager(callBack ExitCallbackFunc, carfileCache *cache.CarfileCache, nodeMgrCache *cache.NodeMgrCache, cdb *persistent.CarfileDB, ndb *persistent.NodeMgrDB) *Manager {
	nodeManager := &Manager{
		nodeQuitCallBack: callBack,
		CarfileDB:        cdb,
		NodeMgrDB:        ndb,
		CarfileCache:     carfileCache,
		NodeMgrCache:     nodeMgrCache,
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
		err := m.NodeMgrDB.UpdateNodeOnlineTime(node.DeviceID, saveInfoInterval*keepaliveTime)
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
		err := m.NodeMgrDB.UpdateNodeOnlineTime(node.DeviceID, saveInfoInterval*keepaliveTime)
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

	err := m.saveInfo(node.BaseInfo)
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

	err := m.saveInfo(node.BaseInfo)
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

func NewSessionCallBackFunc(nodeMgr *Manager) (common.SessionCallbackFunc, error) {
	return func(deviceID, remoteAddr string) {
		lastTime := time.Now()

		edge := nodeMgr.GetEdgeNode(deviceID)
		if edge != nil {
			edge.SetLastRequestTime(lastTime)
			edge.ConnectRPC(remoteAddr, false)
			return
		}

		candidate := nodeMgr.GetCandidateNode(deviceID)
		if candidate != nil {
			candidate.SetLastRequestTime(lastTime)
			candidate.ConnectRPC(remoteAddr, false)
			return
		}
	}, nil
}

// FindNodeDownloadInfos  find device with block cid
func (m *Manager) FindNodeDownloadInfos(cid, userURL string) ([]*api.DownloadInfoResult, error) {
	infos := make([]*api.DownloadInfoResult, 0)

	hash, err := cidutil.CIDString2HashString(cid)
	if err != nil {
		return nil, xerrors.Errorf("%s cid to hash err:%s", cid, err.Error())
	}

	caches, err := m.CarfileDB.CarfileReplicaInfosWithHash(hash, true)
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
	caches, err := m.CarfileDB.CarfileReplicaInfosWithHash(hash, true)
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
	nodes, err := m.NodeMgrDB.LongTimeOfflineNodes(nodeOfflineTime)
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
	err := m.NodeMgrDB.SetNodesQuit(deviceIDs)
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

// node online
func (m *Manager) saveInfo(n *BaseInfo) error {
	n.PrivateKeyStr = titanRsa.PrivateKey2Pem(n.privateKey)
	n.Quitted = false
	n.LastTime = time.Now()

	err := m.NodeMgrDB.UpdateNodeInfo(n.DeviceInfo)
	if err != nil {
		return err
	}

	return nil
}
