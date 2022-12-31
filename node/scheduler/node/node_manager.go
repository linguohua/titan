package node

import (
	"runtime"
	"sync"
	"time"

	"github.com/go-ping/ping"

	logging "github.com/ipfs/go-log/v2"
	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/node/helper"
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
	nodeExitedCallBack  func(string)
	getAuthToken        func() []byte
}

// NewNodeManager New
func NewNodeManager(nodeOfflineCallBack, nodeExitedCallBack func(string), getToken func() []byte) *Manager {
	nodeManager := &Manager{
		nodeOfflineCallBack: nodeOfflineCallBack,
		nodeExitedCallBack:  nodeExitedCallBack,
		getAuthToken:        getToken,
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
	// log.Warnf("%s, lastTime :%s", deviceID, lastTime.String())

	if !lastTime.After(nowTime) {
		// offline
		m.edgeOffline(node)
		node = nil
		return
	}

	_, err := cache.GetDB().IncrNodeOnlineTime(node.DeviceId, keepaliveTime)
	if err != nil {
		log.Warnf("IncrNodeOnlineTime err:%s,deviceID:%s", err.Error(), node.DeviceId)
	}
}

func (m *Manager) candidateKeepalive(node *CandidateNode, nowTime time.Time) {
	lastTime := node.GetLastRequestTime()
	// log.Warnf("%s, lastTime :%s", deviceID, lastTime.String())

	if !lastTime.After(nowTime) {
		// offline
		m.candidateOffline(node)
		node = nil
		return
	}

	_, err := cache.GetDB().IncrNodeOnlineTime(node.DeviceId, keepaliveTime)
	if err != nil {
		log.Warnf("IncrNodeOnlineTime err:%s,deviceID:%s", err.Error(), node.DeviceId)
	}
}

func (m *Manager) checkNodesKeepalive() {
	nowTime := time.Now().Add(-time.Duration(keepaliveTime) * time.Second)
	// log.Warnf("nodeKeepalive nowTime :%s", nowTime.String())

	m.EdgeNodeMap.Range(func(key, value interface{}) bool {
		// deviceID := key.(string)
		node := value.(*EdgeNode)

		if node == nil {
			return true
		}

		go m.edgeKeepalive(node, nowTime)

		// result, err := statisticsPing(node.DeviceInfo.ExternalIp)
		// if err != nil {
		// 	log.Warnf("statistics ping %s: %v", deviceID, err)
		// } else {
		// 	err = cache.GetDB().UpdateDeviceInfo(deviceID, func(deviceInfo *api.DevicesInfo) {
		// 		deviceInfo.PkgLossRatio = result.PacketLoss
		// 		deviceInfo.Latency = float64(result.AvgRtt.Milliseconds())
		// 	})
		// 	if err != nil {
		// 		log.Warnf("update packet loss and lantency: %v", err)
		// 	}
		// }

		return true
	})

	m.CandidateNodeMap.Range(func(key, value interface{}) bool {
		// deviceID := key.(string)
		node := value.(*CandidateNode)

		if node == nil {
			return true
		}

		go m.candidateKeepalive(node, nowTime)
		// result, err := statisticsPing(node.DeviceInfo.ExternalIp)
		// if err != nil {
		// 	log.Warnf("statistics ping %s: %v", deviceID, err)
		// } else {
		// 	err = cache.GetDB().UpdateDeviceInfo(deviceID, func(deviceInfo *api.DevicesInfo) {
		// 		deviceInfo.PkgLossRatio = result.PacketLoss
		// 		deviceInfo.Latency = float64(result.AvgRtt.Milliseconds())
		// 	})
		// 	if err != nil {
		// 		log.Warnf("update packet loss and lantency: %v", err)
		// 	}
		// }

		return true
	})
}

// GetNodes get nodes with type
func (m *Manager) GetNodes(nodeType api.NodeTypeName) ([]string, error) {
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

// // GetAllCandidate get all candidate node
// func (m *Manager) GetAllCandidate() *sync.Map {
// 	return m.candidateNodeMap
// }

// // GetAllEdge  get all edge node
// func (m *Manager) GetAllEdge() *sync.Map {
// 	return m.edgeNodeMap
// }

// EdgeOnline Edge Online
func (m *Manager) EdgeOnline(node *EdgeNode) error {
	deviceID := node.DeviceId

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
	nodeI, ok := m.EdgeNodeMap.Load(deviceID)
	if ok && nodeI != nil {
		node := nodeI.(*EdgeNode)

		return node
	}

	return nil
}

func (m *Manager) edgeOffline(node *EdgeNode) {
	deviceID := node.DeviceId
	// close old node
	node.ClientCloser()

	log.Warnf("edgeOffline :%s", deviceID)

	m.EdgeNodeMap.Delete(deviceID)

	node.setNodeOffline()

	if m.nodeOfflineCallBack != nil {
		m.nodeOfflineCallBack(deviceID)
	}
}

// CandidateOnline Candidate Online
func (m *Manager) CandidateOnline(node *CandidateNode) error {
	deviceID := node.DeviceId

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
	nodeI, ok := m.CandidateNodeMap.Load(deviceID)
	if ok && nodeI != nil {
		node := nodeI.(*CandidateNode)

		return node
	}

	return nil
}

func (m *Manager) candidateOffline(node *CandidateNode) {
	deviceID := node.DeviceId
	// close old node
	node.ClientCloser()

	log.Warnf("candidateOffline :%s", deviceID)

	m.CandidateNodeMap.Delete(deviceID)

	node.setNodeOffline()

	if m.nodeOfflineCallBack != nil {
		m.nodeOfflineCallBack(deviceID)
	}
}

// // FindEdgesByList find edges by list
// func (m *Manager) FindEdgesByList(list []string, filterMap map[string]string) []*EdgeNode {
// 	if filterMap == nil {
// 		filterMap = make(map[string]string)
// 	}

// 	nodes := make([]*EdgeNode, 0)

// 	for _, dID := range list {
// 		if _, exist := filterMap[dID]; exist {
// 			continue
// 		}

// 		node := m.GetEdgeNode(dID)
// 		if node != nil {
// 			nodes = append(nodes, node)
// 		}
// 	}

// 	if len(nodes) > 0 {
// 		return nodes
// 	}

// 	return nil
// }

// // FindEdges Find Edges by all edge
// func (m *Manager) FindEdges(filterMap map[string]string) []*EdgeNode {
// 	if filterMap == nil {
// 		filterMap = make(map[string]string)
// 	}

// 	nodes := make([]*EdgeNode, 0)

// 	m.EdgeNodeMap.Range(func(key, value interface{}) bool {
// 		deviceID := key.(string)
// 		node := value.(*EdgeNode)

// 		if _, ok := filterMap[deviceID]; ok {
// 			return true
// 		}

// 		if node == nil {
// 			return true
// 		}
// 		nodes = append(nodes, node)

// 		return true
// 	})

// 	if len(nodes) > 0 {
// 		return nodes
// 	}

// 	return nil
// }

// FindCandidates Find CandidateNodes from all Candidate and filter filterMap
func (m *Manager) FindCandidates(filterMap map[string]string) []*CandidateNode {
	if filterMap == nil {
		filterMap = make(map[string]string)
	}

	nodes := make([]*CandidateNode, 0)

	m.CandidateNodeMap.Range(func(key, value interface{}) bool {
		deviceID := key.(string)
		node := value.(*CandidateNode)

		if _, ok := filterMap[deviceID]; ok {
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
		if _, ok := filterMap[dID]; ok {
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
func (m *Manager) FindNodeDownloadInfos(cid string) ([]api.DownloadInfoResult, error) {
	infos := make([]api.DownloadInfoResult, 0)

	hash, err := helper.CIDString2HashString(cid)
	if err != nil {
		return nil, xerrors.Errorf("%s cid to hash err:%s", cid, err.Error())
	}

	deviceIDs, err := persistent.GetDB().GetNodesWithBlock(hash, true)
	if err != nil {
		return nil, err
	}

	if len(deviceIDs) <= 0 {
		return nil, xerrors.Errorf("not found node , with hash:%s", hash)
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

// GetCandidatesWithBlockHash find candidates with block hash
func (m *Manager) GetCandidatesWithBlockHash(hash, filterHash string) ([]*CandidateNode, error) {
	deviceIDs, err := persistent.GetDB().GetNodesWithBlock(hash, true)
	if err != nil {
		return nil, err
	}
	// log.Infof("getCandidateNodesWithData deviceIDs : %v", deviceIDs)

	if len(deviceIDs) <= 0 {
		return nil, xerrors.Errorf("not found node, with hash:%s", hash)
	}

	filterMap := make(map[string]string)

	if filterHash != "" {
		filterMap[filterHash] = hash
	}

	nodeCs := m.FindCandidatesByList(deviceIDs, filterMap)

	return nodeCs, nil
}

func (m *Manager) checkNodesQuit() {
	nodes, err := persistent.GetDB().GetOfflineNodes()
	if err != nil {
		log.Warnf("checkNodesQuit GetOfflineNodes err:%s", err.Error())
		return
	}

	t := time.Now().Add(-time.Duration(nodeExitTime) * time.Hour)

	exiteds := make([]string, 0)

	for _, node := range nodes {
		if node.LastTime.After(t) {
			continue
		}

		// node quitted
		exiteds = append(exiteds, node.DeviceID)

		// clean node cache
		go m.NodeQuit(node.DeviceID)
	}
}

// NodeQuit Node quit
func (m *Manager) NodeQuit(deviceID string) {
	err := persistent.GetDB().SetNodeQuit(deviceID)
	if err != nil {
		log.Warnf("NodeExited SetNodeQuit err:%s", err.Error())
		return
	}

	if m.nodeExitedCallBack != nil {
		m.nodeExitedCallBack(deviceID)
	}
}

// GetAuthToken get token
func (m *Manager) GetAuthToken() []byte {
	return m.getAuthToken()
}

// FindDownloadinfoForBlocks  filter cached blocks and find download url from candidate
// func (m *Manager) FindDownloadinfoForBlocks(blocks []*api.BlockCacheInfo, carfileHash, cacheID string) []api.ReqCacheData {
// 	reqList := make([]api.ReqCacheData, 0)
// 	notFindCandidateBlocks := make([]api.BlockCacheInfo, 0)

// 	csMap := make(map[string][]api.BlockCacheInfo)
// 	for _, block := range blocks {
// 		deviceID := block.From

// 		list, ok := csMap[deviceID]
// 		if !ok {
// 			list = make([]api.BlockCacheInfo, 0)
// 		}
// 		list = append(list, *block)

// 		csMap[deviceID] = list
// 	}

// 	for deviceID, list := range csMap {
// 		// info, err := persistent.GetDB().GetNodeAuthInfo(deviceID)

// 		node := m.GetCandidateNode(deviceID)
// 		if node != nil {
// 			reqList = append(reqList, api.ReqCacheData{BlockInfos: list, DownloadURL: node.GetAddress(), DownloadToken: string(m.getAuthToken()), CardFileHash: carfileHash, CacheID: cacheID})

// 			// tk, err := token.GenerateToken(info.PrivateKey, time.Now().Add(helper.DownloadTokenExpireAfter).Unix())
// 			// if err == nil {
// 			// 	reqList = append(reqList, api.ReqCacheData{BlockInfos: list, DownloadURL: info.URL, DownloadToken: tk, CardFileHash: carfileHash, CacheID: cacheID})

// 			continue
// 			// }
// 		}

// 		notFindCandidateBlocks = append(notFindCandidateBlocks, list...)
// 	}

// 	if len(notFindCandidateBlocks) > 0 {
// 		reqList = append(reqList, api.ReqCacheData{BlockInfos: notFindCandidateBlocks, CardFileHash: carfileHash, CacheID: cacheID})
// 	}

// 	return reqList
// }

func statisticsPing(ip string) (*ping.Statistics, error) {
	pinger, err := ping.NewPinger(ip)
	if err != nil {
		return nil, err
	}

	if runtime.GOOS == "windows" {
		pinger.SetPrivileged(true)
	}

	pinger.Count = 3
	pinger.Timeout = 5 * time.Second
	err = pinger.Run()
	if err != nil {
		return nil, err
	}

	return pinger.Statistics(), nil
}
