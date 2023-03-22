package node

import (
	"context"
	"crypto"
	"crypto/rsa"
	"sync"
	"time"

	"github.com/linguohua/titan/api/types"
	"github.com/linguohua/titan/node/modules/dtypes"

	logging "github.com/ipfs/go-log/v2"
	"github.com/linguohua/titan/node/cidutil"
	titanrsa "github.com/linguohua/titan/node/rsa"
	"github.com/linguohua/titan/node/scheduler/db"
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
	*db.SqlDB

	*rsa.PrivateKey
	dtypes.ServerID
}

// NewManager return new node manager instance
func NewManager(ndb *db.SqlDB, serverID dtypes.ServerID, k *rsa.PrivateKey) *Manager {
	nodeManager := &Manager{
		SqlDB:      ndb,
		ServerID:   serverID,
		PrivateKey: k,
	}

	go nodeManager.run()

	return nodeManager
}

func (m *Manager) run() {
	ticker := time.NewTicker(time.Duration(keepaliveTime) * time.Second)
	defer ticker.Stop()

	count := 0

	for {
		<-ticker.C
		count++
		isSave := count%saveInfoInterval == 0

		m.checkNodesKeepalive(isSave)

		// check node offline how time
		m.checkWhetherNodeQuits()
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
		err := m.UpdateNodeOnlineTime(node.NodeID, saveInfoInterval*keepaliveTime)
		if err != nil {
			log.Errorf("UpdateNodeOnlineTime err:%s,nodeID:%s", err.Error(), node.NodeID)
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
		err := m.UpdateNodeOnlineTime(node.NodeID, saveInfoInterval*keepaliveTime)
		if err != nil {
			log.Errorf("UpdateNodeOnlineTime err:%s,nodeID:%s", err.Error(), node.NodeID)
		}
	}
}

func (m *Manager) checkNodesKeepalive(isSave bool) {
	nowTime := time.Now().Add(-time.Duration(keepaliveTime) * time.Second)

	m.EdgeNodes.Range(func(key, value interface{}) bool {
		// nodeID := key.(string)
		node := value.(*Edge)

		if node == nil {
			return true
		}

		go m.edgeKeepalive(node, nowTime, isSave)

		return true
	})

	m.CandidateNodes.Range(func(key, value interface{}) bool {
		// nodeID := key.(string)
		node := value.(*Candidate)

		if node == nil {
			return true
		}

		go m.candidateKeepalive(node, nowTime, isSave)

		return true
	})
}

// OnlineNodeList get nodes with type
func (m *Manager) OnlineNodeList(nodeType types.NodeType) ([]string, error) {
	list := make([]string, 0)

	if nodeType == types.NodeUnknown || nodeType == types.NodeCandidate {
		m.CandidateNodes.Range(func(key, value interface{}) bool {
			nodeID := key.(string)
			list = append(list, nodeID)

			return true
		})
	}

	if nodeType == types.NodeUnknown || nodeType == types.NodeEdge {
		m.EdgeNodes.Range(func(key, value interface{}) bool {
			nodeID := key.(string)
			list = append(list, nodeID)

			return true
		})
	}

	return list, nil
}

// EdgeOnline Edge Online
func (m *Manager) EdgeOnline(node *Edge) error {
	nodeID := node.NodeID

	nodeOld := m.GetEdgeNode(nodeID)
	if nodeOld != nil {
		nodeOld.ClientCloser()

		nodeOld = nil
	}

	err := m.saveInfo(node.BaseInfo)
	if err != nil {
		return err
	}

	m.EdgeNodes.Store(nodeID, node)

	return nil
}

// GetEdgeNode Get EdgeNode
func (m *Manager) GetEdgeNode(nodeID string) *Edge {
	nodeI, exist := m.EdgeNodes.Load(nodeID)
	if exist && nodeI != nil {
		node := nodeI.(*Edge)

		return node
	}

	return nil
}

func (m *Manager) edgeOffline(node *Edge) {
	nodeID := node.NodeID
	// close old node
	node.ClientCloser()

	log.Infof("Edge Offline :%s", nodeID)

	m.EdgeNodes.Delete(nodeID)

	// notify locator
	locator.ChangeNodeOnlineStatus(nodeID, false)
}

// CandidateOnline Candidate Online
func (m *Manager) CandidateOnline(node *Candidate) error {
	nodeID := node.NodeID

	nodeOld := m.GetCandidateNode(nodeID)
	if nodeOld != nil {
		nodeOld.ClientCloser()

		nodeOld = nil
	}

	err := m.saveInfo(node.BaseInfo)
	if err != nil {
		return err
	}

	m.CandidateNodes.Store(nodeID, node)

	// update validator owner
	return m.UpdateValidatorInfo(m.ServerID, nodeID)
}

// GetCandidateNode Get Candidate Node
func (m *Manager) GetCandidateNode(nodeID string) *Candidate {
	nodeI, exist := m.CandidateNodes.Load(nodeID)
	if exist && nodeI != nil {
		node := nodeI.(*Candidate)

		return node
	}

	return nil
}

func (m *Manager) candidateOffline(node *Candidate) {
	nodeID := node.NodeID
	// close old node
	node.ClientCloser()

	log.Infof("Candidate Offline :%s", nodeID)

	m.CandidateNodes.Delete(nodeID)

	// notify locator
	locator.ChangeNodeOnlineStatus(nodeID, false)
}

// FindCandidates Find CandidateNodes from all Candidate and filter filterMap
func (m *Manager) FindCandidates(filterMap map[string]string) []*Candidate {
	if filterMap == nil {
		filterMap = make(map[string]string)
	}

	nodes := make([]*Candidate, 0)

	m.CandidateNodes.Range(func(key, value interface{}) bool {
		nodeID := key.(string)
		node := value.(*Candidate)

		if _, exist := filterMap[nodeID]; exist {
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

// NodeSessionCallBack node ping pong
func (m *Manager) NodeSessionCallBack(nodeID, remoteAddr string) {
	lastTime := time.Now()

	edge := m.GetEdgeNode(nodeID)
	if edge != nil {
		edge.SetLastRequestTime(lastTime)
		edge.ConnectRPC(remoteAddr, false)
		return
	}

	candidate := m.GetCandidateNode(nodeID)
	if candidate != nil {
		candidate.SetLastRequestTime(lastTime)
		candidate.ConnectRPC(remoteAddr, false)
		return
	}
}

func NewSessionCallBackFunc(nodeMgr *Manager) (dtypes.SessionCallbackFunc, error) {
	return func(nodeID, remoteAddr string) {
		lastTime := time.Now()

		edge := nodeMgr.GetEdgeNode(nodeID)
		if edge != nil {
			edge.SetLastRequestTime(lastTime)
			edge.ConnectRPC(remoteAddr, false)
			return
		}

		candidate := nodeMgr.GetCandidateNode(nodeID)
		if candidate != nil {
			candidate.SetLastRequestTime(lastTime)
			candidate.ConnectRPC(remoteAddr, false)
			return
		}
	}, nil
}

// FindNodeDownloadInfos  find edges with block cid
func (m *Manager) FindNodeDownloadInfos(cid, userURL string) ([]*types.DownloadInfo, error) {
	hash, err := cidutil.CIDString2HashString(cid)
	if err != nil {
		return nil, xerrors.Errorf("%s cid to hash err:%s", cid, err.Error())
	}

	replicas, err := m.LoadSucceededReplicas(hash, types.NodeEdge)
	if err != nil {
		return nil, err
	}

	if len(replicas) <= 0 {
		return nil, xerrors.Errorf("not found node , with :%s", cid)
	}

	titanRsa := titanrsa.New(crypto.SHA256, crypto.SHA256.New())
	infos := make([]*types.DownloadInfo, 0)

	for _, nodeID := range replicas {
		eNode := m.GetEdgeNode(nodeID)
		if eNode == nil {
			continue
		}

		err := eNode.nodeAPI.UserNATTravel(context.Background(), userURL)
		if err != nil {
			continue
		}

		credentials, err := eNode.Credentials(cid, titanRsa, m.PrivateKey)
		if err != nil {
			continue
		}

		infos = append(infos, &types.DownloadInfo{URL: eNode.DownloadURL(), NodeID: nodeID, Credentials: credentials})
	}

	return infos, nil
}

// GetCandidatesWithBlockHash find candidates with block hash
func (m *Manager) GetCandidatesWithBlockHash(hash, filterNode string) ([]*Candidate, error) {
	replicas, err := m.LoadSucceededReplicas(hash, types.NodeCandidate)
	if err != nil {
		return nil, err
	}

	if len(replicas) <= 0 {
		return nil, xerrors.Errorf("not found node, with hash:%s", hash)
	}

	nodes := make([]*Candidate, 0)

	for _, dID := range replicas {
		if dID == filterNode {
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
	nodes, err := m.LoadTimeoutNodes(nodeOfflineTime)
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
		quits = append(quits, node.NodeID)
	}

	if len(quits) > 0 {
		m.NodesQuit(quits)
	}
}

// NodesQuit Nodes quit
func (m *Manager) NodesQuit(nodeIDs []string) {
	err := m.SetNodesQuit(nodeIDs)
	if err != nil {
		log.Errorf("NodeExited SetNodesQuit err:%s", err.Error())
		return
	}

	log.Infof("node event , nodes quit:%v", nodeIDs)

	hashes, err := m.LoadCarfileRecordsOfNodes(nodeIDs)
	if err != nil {
		log.Errorf("LoadCarfileRecordsWithNodes err:%s", err.Error())
		return
	}

	err = m.RemoveReplicaInfoOfNodes(nodeIDs)
	if err != nil {
		log.Errorf("RemoveReplicaInfoWithNodes err:%s", err.Error())
		return
	}

	for _, hash := range hashes {
		log.Infof("need restore storage :%s", hash)
	}
}

// GetNode get node
func (m *Manager) GetNode(nodeID string) *BaseInfo {
	edge := m.GetEdgeNode(nodeID)
	if edge != nil {
		return edge.BaseInfo
	}

	candidate := m.GetCandidateNode(nodeID)
	if candidate != nil {
		return candidate.BaseInfo
	}

	return nil
}

// node online
func (m *Manager) saveInfo(n *BaseInfo) error {
	n.Quitted = false
	n.LastTime = time.Now()

	err := m.UpdateNodeInfo(n.NodeInfo)
	if err != nil {
		return err
	}

	return nil
}
