package node

import (
	"crypto/rsa"
	"runtime"
	"sync"
	"time"

	"github.com/go-ping/ping"

	logging "github.com/ipfs/go-log/v2"
	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/node/helper"
	titanRsa "github.com/linguohua/titan/node/rsa"
	"github.com/linguohua/titan/node/scheduler/area"
	"github.com/linguohua/titan/node/scheduler/db/cache"
	"github.com/linguohua/titan/node/scheduler/db/persistent"
	"golang.org/x/xerrors"
)

var log = logging.Logger("node")

const (
	nodeExitTime = 24 // If it is not online after this time, it is determined that the node has exited the system (hour)

	keepaliveTime = 30 // keepalive time interval (second)
)

// Manager Node Manager
type Manager struct {
	edgeNodeMap      sync.Map
	candidateNodeMap sync.Map

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
			m.checkNodesExited()
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

	_, err := cache.GetDB().IncrNodeOnlineTime(node.GetDeviceInfo().DeviceId, keepaliveTime)
	if err != nil {
		log.Warnf("IncrNodeOnlineTime err:%s,deviceID:%s", err.Error(), node.GetDeviceInfo().DeviceId)
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

	_, err := cache.GetDB().IncrNodeOnlineTime(node.GetDeviceInfo().DeviceId, keepaliveTime)
	if err != nil {
		log.Warnf("IncrNodeOnlineTime err:%s,deviceID:%s", err.Error(), node.GetDeviceInfo().DeviceId)
	}
}

func (m *Manager) checkNodesKeepalive() {
	nowTime := time.Now().Add(-time.Duration(keepaliveTime) * time.Second)
	// log.Warnf("nodeKeepalive nowTime :%s", nowTime.String())

	m.edgeNodeMap.Range(func(key, value interface{}) bool {
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

	m.candidateNodeMap.Range(func(key, value interface{}) bool {
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
		m.candidateNodeMap.Range(func(key, value interface{}) bool {
			deviceID := key.(string)
			list = append(list, deviceID)

			return true
		})
	}

	if nodeType == api.TypeNameAll || nodeType == api.TypeNameEdge {
		m.edgeNodeMap.Range(func(key, value interface{}) bool {
			deviceID := key.(string)
			list = append(list, deviceID)

			return true
		})
	}

	return list, nil
}

// GetAllCandidate get all candidate node
func (m *Manager) GetAllCandidate() []*CandidateNode {
	list := make([]*CandidateNode, 0)

	m.candidateNodeMap.Range(func(key, value interface{}) bool {
		c := value.(*CandidateNode)
		list = append(list, c)

		return true
	})

	return list
}

// GetAllEdge  get all edge node
func (m *Manager) GetAllEdge() []*EdgeNode {
	list := make([]*EdgeNode, 0)

	m.edgeNodeMap.Range(func(key, value interface{}) bool {
		e := value.(*EdgeNode)
		list = append(list, e)

		return true
	})

	return list
}

// EdgeOnline Edge Online
func (m *Manager) EdgeOnline(node *EdgeNode) error {
	deviceID := node.GetDeviceInfo().DeviceId

	geoInfo, isOk := area.GetGeoInfoWithIP(node.GetDeviceInfo().ExternalIp)
	if !isOk {
		log.Errorf("edgeOnline err DeviceId:%s,ip%s,geo:%s", deviceID, node.GetDeviceInfo().ExternalIp, geoInfo.Geo)
		return xerrors.New("Area not exist")
	}

	node.SetGeoInfo(geoInfo)

	nodeOld := m.GetEdgeNode(deviceID)
	if nodeOld != nil {
		nodeOld.ClientCloser()

		nodeOld = nil
	}

	err := node.setNodeOnline()
	if err != nil {
		return err
	}

	m.edgeNodeMap.Store(deviceID, node)

	return nil
}

// GetEdgeNode Get EdgeNode
func (m *Manager) GetEdgeNode(deviceID string) *EdgeNode {
	nodeI, ok := m.edgeNodeMap.Load(deviceID)
	if ok && nodeI != nil {
		node := nodeI.(*EdgeNode)

		return node
	}

	return nil
}

func (m *Manager) edgeOffline(node *EdgeNode) {
	deviceID := node.GetDeviceInfo().DeviceId
	// close old node
	node.ClientCloser()

	log.Warnf("edgeOffline :%s", deviceID)

	m.edgeNodeMap.Delete(deviceID)

	node.setNodeOffline()

	if m.nodeOfflineCallBack != nil {
		m.nodeOfflineCallBack(deviceID)
	}
}

// CandidateOnline Candidate Online
func (m *Manager) CandidateOnline(node *CandidateNode) error {
	deviceID := node.GetDeviceInfo().DeviceId

	geoInfo, isOk := area.GetGeoInfoWithIP(node.GetDeviceInfo().ExternalIp)
	if !isOk {
		log.Errorf("candidateOnline err DeviceId:%s,ip%s,geo:%s", deviceID, node.GetDeviceInfo().ExternalIp, geoInfo.Geo)
		return xerrors.New("Area not exist")
	}

	node.SetGeoInfo(geoInfo)

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

	m.candidateNodeMap.Store(deviceID, node)

	return nil
}

// GetCandidateNode Get Candidate Node
func (m *Manager) GetCandidateNode(deviceID string) *CandidateNode {
	nodeI, ok := m.candidateNodeMap.Load(deviceID)
	if ok && nodeI != nil {
		node := nodeI.(*CandidateNode)

		return node
	}

	return nil
}

func (m *Manager) candidateOffline(node *CandidateNode) {
	deviceID := node.GetDeviceInfo().DeviceId
	// close old node
	node.ClientCloser()

	log.Warnf("candidateOffline :%s", deviceID)

	m.candidateNodeMap.Delete(deviceID)

	node.setNodeOffline()

	if m.nodeOfflineCallBack != nil {
		m.nodeOfflineCallBack(deviceID)
	}
}

// FindEdgeNodes Find EdgeNodes from useDeviceIDs and skip skips
func (m *Manager) FindEdgeNodes(useDeviceIDs []string, skips map[string]string) []*EdgeNode {
	if skips == nil {
		skips = make(map[string]string)
	}

	list := make([]*EdgeNode, 0)

	if useDeviceIDs != nil && len(useDeviceIDs) > 0 {
		for _, dID := range useDeviceIDs {
			if _, ok := skips[dID]; ok {
				continue
			}

			node := m.GetEdgeNode(dID)
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

// FindCandidateNodes Find CandidateNodes from useDeviceIDs and skip skips
func (m *Manager) FindCandidateNodes(useDeviceIDs []string, skips map[string]string) []*CandidateNode {
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
			node := m.GetCandidateNode(dID)
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

	deviceIDs, err := persistent.GetDB().GetNodesWithCache(hash, true)
	if err != nil {
		return nil, err
	}

	if len(deviceIDs) <= 0 {
		return nil, xerrors.Errorf("Not Found Node , with hash:%s", hash)
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

// GetCandidateNodesWithData find device with block hash
func (m *Manager) GetCandidateNodesWithData(hash, skip string) ([]*CandidateNode, error) {
	deviceIDs, err := persistent.GetDB().GetNodesWithCache(hash, true)
	if err != nil {
		return nil, err
	}
	// log.Infof("getCandidateNodesWithData deviceIDs : %v", deviceIDs)

	if len(deviceIDs) <= 0 {
		return nil, xerrors.Errorf("Not Found Node, with hash:%s", hash)
	}

	skips := make(map[string]string)

	if skip != "" {
		skips[skip] = hash
	}

	nodeCs := m.FindCandidateNodes(deviceIDs, skips)

	return nodeCs, nil
}

// SetDeviceInfo Set Device Info
func (m *Manager) SetDeviceInfo(deviceID string, info api.DevicesInfo) error {
	_, err := cache.GetDB().SetDeviceInfo(deviceID, info)
	if err != nil {
		log.Errorf("set device info: %s", err.Error())
		return err
	}
	return nil
}

// IsDeviceExist Check if the id exists
func (m *Manager) IsDeviceExist(deviceID string, nodeType int) bool {
	var nType int
	err := persistent.GetDB().GetRegisterInfo(deviceID, "node_type", &nType)
	if err != nil {
		return false
	}

	if nodeType != 0 {
		return nType == nodeType
	}

	return true
}

func (m *Manager) getDeviccePrivateKey(deviceID string, authInfo *api.DownloadServerAccessAuth) (*rsa.PrivateKey, error) {
	edge := m.GetEdgeNode(deviceID)
	if edge != nil {
		return edge.GetPrivateKey(), nil
	}

	candidate := m.GetCandidateNode(deviceID)
	if candidate != nil {
		return candidate.GetPrivateKey(), nil
	}

	privateKey, err := titanRsa.Pem2PrivateKey(authInfo.PrivateKey)
	if err != nil {
		return nil, err
	}

	return privateKey, nil
}

func (m *Manager) checkNodesExited() {
	nodes, err := persistent.GetDB().GetOfflineNodes()
	if err != nil {
		log.Warnf("checkNodesExited GetOfflineNodes err:%s", err.Error())
		return
	}

	t := time.Now().Add(-time.Duration(nodeExitTime) * time.Hour)

	exiteds := make([]string, 0)

	for _, node := range nodes {
		if node.LastTime.After(t) {
			continue
		}

		// node exited
		exiteds = append(exiteds, node.DeviceID)

		// clean node cache
		go m.NodeExited(node.DeviceID)
	}
}

// NodeExited Node Exited
func (m *Manager) NodeExited(deviceID string) {
	err := persistent.GetDB().SetNodeExited(deviceID)
	if err != nil {
		log.Warnf("NodeExited SetNodeExited err:%s", err.Error())
		return
	}

	if m.nodeExitedCallBack != nil {
		m.nodeExitedCallBack(deviceID)
	}
}

// FindDownloadinfoForBlocks  filter cached blocks and find download url from candidate
func (m *Manager) FindDownloadinfoForBlocks(blocks []api.BlockCacheInfo, carfileHash, cacheID string) []api.ReqCacheData {
	reqList := make([]api.ReqCacheData, 0)
	notFindCandidateBlocks := make([]api.BlockCacheInfo, 0)

	csMap := make(map[string][]api.BlockCacheInfo)
	for _, block := range blocks {
		deviceID := block.From

		list, ok := csMap[deviceID]
		if !ok {
			list = make([]api.BlockCacheInfo, 0)
		}
		list = append(list, block)

		csMap[deviceID] = list
	}

	for deviceID, list := range csMap {
		// info, err := persistent.GetDB().GetNodeAuthInfo(deviceID)

		node := m.GetCandidateNode(deviceID)
		if node != nil {
			reqList = append(reqList, api.ReqCacheData{BlockInfos: list, DownloadURL: node.GetAddress(), DownloadToken: string(m.getAuthToken()), CardFileHash: carfileHash, CacheID: cacheID})

			// tk, err := token.GenerateToken(info.PrivateKey, time.Now().Add(helper.DownloadTokenExpireAfter).Unix())
			// if err == nil {
			// 	reqList = append(reqList, api.ReqCacheData{BlockInfos: list, DownloadURL: info.URL, DownloadToken: tk, CardFileHash: carfileHash, CacheID: cacheID})

			continue
			// }
		}

		notFindCandidateBlocks = append(notFindCandidateBlocks, list...)
	}

	if len(notFindCandidateBlocks) > 0 {
		reqList = append(reqList, api.ReqCacheData{BlockInfos: notFindCandidateBlocks, CardFileHash: carfileHash, CacheID: cacheID})
	}

	return reqList
}

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
