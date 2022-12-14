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
	"github.com/linguohua/titan/node/scheduler/errmsg"
	"golang.org/x/xerrors"
)

var log = logging.Logger("node")

const exitTime = 24 // If it is not online after this time, it is determined that the node has exited the system (hour)

// Manager Node Manager
type Manager struct {
	EdgeNodeMap      sync.Map
	CandidateNodeMap sync.Map

	keepaliveTime float64 // keepalive time interval (minute)

	nodeOfflineCallBack func(string)
	nodeExitedCallBack  func(string)
	authNew             func() ([]byte, error)
}

// NewNodeManager New
func NewNodeManager(nodeOfflineCallBack, nodeExitedCallBack func(string), authNew func() ([]byte, error)) *Manager {
	nodeManager := &Manager{
		keepaliveTime:       0.5,
		nodeOfflineCallBack: nodeOfflineCallBack,
		nodeExitedCallBack:  nodeExitedCallBack,
		authNew:             authNew,
	}

	go nodeManager.run()

	return nodeManager
}

func (m *Manager) run() {
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

func (m *Manager) nodeKeepalive() {
	nowTime := time.Now().Add(-time.Duration(m.keepaliveTime*60) * time.Second)
	// log.Warnf("nodeKeepalive nowTime :%s", nowTime.String())

	m.EdgeNodeMap.Range(func(key, value interface{}) bool {
		deviceID := key.(string)
		node := value.(*EdgeNode)

		if node == nil {
			return true
		}

		lastTime := node.LastRequestTime
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

		result, err := statisticsPing(node.DeviceInfo.ExternalIp)
		if err != nil {
			log.Warnf("statistics ping %s: %v", deviceID, err)
		} else {
			err = cache.GetDB().UpdateDeviceInfo(deviceID, func(deviceInfo *api.DevicesInfo) {
				deviceInfo.PkgLossRatio = result.PacketLoss
				deviceInfo.Latency = float64(result.AvgRtt.Milliseconds())
			})
			if err != nil {
				log.Warnf("update packet loss and lantency: %v", err)
			}
		}

		return true
	})

	m.CandidateNodeMap.Range(func(key, value interface{}) bool {
		deviceID := key.(string)
		node := value.(*CandidateNode)

		if node == nil {
			return true
		}

		lastTime := node.LastRequestTime

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

		result, err := statisticsPing(node.DeviceInfo.ExternalIp)
		if err != nil {
			log.Warnf("statistics ping %s: %v", deviceID, err)
		} else {
			err = cache.GetDB().UpdateDeviceInfo(deviceID, func(deviceInfo *api.DevicesInfo) {
				deviceInfo.PkgLossRatio = result.PacketLoss
				deviceInfo.Latency = float64(result.AvgRtt.Milliseconds())
			})
			if err != nil {
				log.Warnf("update packet loss and lantency: %v", err)
			}
		}

		return true
	})

	// err := persistent.GetDB().AddAllNodeOnlineTime(int64(m.keepaliveTime))
	// if err != nil {
	// 	log.Warnf("AddAllNodeOnlineTime err:%v", err.Error())
	// }
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

// GetAllCandidate get nodes
func (m *Manager) GetAllCandidate() []*CandidateNode {
	list := make([]*CandidateNode, 0)

	m.CandidateNodeMap.Range(func(key, value interface{}) bool {
		c := value.(*CandidateNode)
		list = append(list, c)

		return true
	})

	return list
}

// GetAllEdge get nodes
func (m *Manager) GetAllEdge() []*EdgeNode {
	list := make([]*EdgeNode, 0)

	m.EdgeNodeMap.Range(func(key, value interface{}) bool {
		e := value.(*EdgeNode)
		list = append(list, e)

		return true
	})

	return list
}

// EdgeOnline Edge Online
func (m *Manager) EdgeOnline(node *EdgeNode) error {
	deviceID := node.DeviceInfo.DeviceId

	isOk, geoInfo := area.IPLegality(node.DeviceInfo.ExternalIp)
	if !isOk {
		log.Errorf("edgeOnline err DeviceId:%s,ip%s,geo:%s", deviceID, node.DeviceInfo.ExternalIp, geoInfo.Geo)
		return xerrors.New(errmsg.ErrAreaNotExist)
	}

	node.GeoInfo = geoInfo

	nodeOld := m.GetEdgeNode(deviceID)
	if nodeOld != nil {
		nodeOld.Closer()

		nodeOld = nil
	}

	err := node.setNodeOnline(api.TypeNameEdge)
	if err != nil {
		return err
	}

	m.EdgeNodeMap.Store(deviceID, node)
	// m.areaManager.addEdge(node)

	// m.validatePool.addPendingNode(node, nil)

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

func (m *Manager) getNodeArea(deviceID string) string {
	e := m.GetEdgeNode(deviceID)
	if e != nil {
		return e.GeoInfo.Geo
	}

	c := m.GetCandidateNode(deviceID)
	if c != nil {
		return c.GeoInfo.Geo
	}

	return ""
}

func (m *Manager) edgeOffline(node *EdgeNode) {
	deviceID := node.DeviceInfo.DeviceId
	// close old node
	node.Closer()

	log.Warnf("edgeOffline :%s", deviceID)

	m.EdgeNodeMap.Delete(deviceID)
	// m.areaManager.removeEdge(node)
	// m.validatePool.removeEdge(deviceID)

	node.setNodeOffline(deviceID, node.GeoInfo, api.TypeNameEdge, node.LastRequestTime)

	if m.nodeOfflineCallBack != nil {
		// m.locatorManager.notifyNodeStatusToLocator(deviceID, false)

		m.nodeOfflineCallBack(deviceID)
	}
}

// CandidateOnline Candidate Online
func (m *Manager) CandidateOnline(node *CandidateNode) error {
	deviceID := node.DeviceInfo.DeviceId

	isOk, geoInfo := area.IPLegality(node.DeviceInfo.ExternalIp)
	if !isOk {
		log.Errorf("candidateOnline err DeviceId:%s,ip%s,geo:%s", deviceID, node.DeviceInfo.ExternalIp, geoInfo.Geo)
		return xerrors.New(errmsg.ErrAreaNotExist)
	}

	node.GeoInfo = geoInfo

	nodeOld := m.GetCandidateNode(deviceID)
	if nodeOld != nil {
		nodeOld.Closer()

		nodeOld = nil
	}

	err := node.setNodeOnline(api.TypeNameCandidate)
	if err != nil {
		// log.Errorf("addCandidateNode NodeOnline err:%v", err)
		return err
	}

	m.CandidateNodeMap.Store(deviceID, node)
	// m.areaManager.addCandidate(node)

	// m.validatePool.addPendingNode(nil, node)

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
	deviceID := node.DeviceInfo.DeviceId
	// close old node
	node.Closer()

	log.Warnf("candidateOffline :%s", deviceID)

	m.CandidateNodeMap.Delete(deviceID)
	// m.areaManager.removeCandidate(node)

	// m.validatePool.removeCandidate(deviceID)

	node.setNodeOffline(deviceID, node.GeoInfo, api.TypeNameCandidate, node.LastRequestTime)

	if m.nodeOfflineCallBack != nil {
		// m.locatorManager.notifyNodeStatusToLocator(deviceID, false)

		m.nodeOfflineCallBack(deviceID)
	}
}

// FindEdgeNodes Find EdgeNodes
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
		m.EdgeNodeMap.Range(func(key, value interface{}) bool {
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

// FindCandidateNodes Find CandidateNodes
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
		m.CandidateNodeMap.Range(func(key, value interface{}) bool {
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
		edge.LastRequestTime = lastTime
		return
	}

	candidate := m.GetCandidateNode(deviceID)
	if candidate != nil {
		candidate.LastRequestTime = lastTime
		return
	}
}

// FindNodeDownloadInfos  find device
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
		return nil, xerrors.Errorf("%s , with hash:%s", errmsg.ErrNodeNotFind, hash)
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

// GetCandidateNodesWithData find device
func (m *Manager) GetCandidateNodesWithData(hash, skip string) ([]*CandidateNode, error) {
	deviceIDs, err := persistent.GetDB().GetNodesWithCache(hash, true)
	if err != nil {
		return nil, err
	}
	// log.Infof("getCandidateNodesWithData deviceIDs : %v", deviceIDs)

	if len(deviceIDs) <= 0 {
		return nil, xerrors.Errorf("%s , with hash:%s", errmsg.ErrNodeNotFind, hash)
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

// IsDeviceExist node exist
func (m *Manager) IsDeviceExist(deviceID string, nodeType int) bool {
	info, err := persistent.GetDB().GetRegisterInfo(deviceID)
	if err != nil {
		return false
	}

	if nodeType != 0 {
		return info.NodeType == nodeType
	}

	return true
}

func (m *Manager) getDeviccePrivateKey(deviceID string, authInfo *api.DownloadServerAccessAuth) (*rsa.PrivateKey, error) {
	edge := m.GetEdgeNode(deviceID)
	if edge != nil {
		return edge.PrivateKey, nil
	}

	candidate := m.GetCandidateNode(deviceID)
	if candidate != nil {
		return candidate.PrivateKey, nil
	}

	privateKey, err := titanRsa.Pem2PrivateKey(authInfo.PrivateKey)
	if err != nil {
		return nil, err
	}

	return privateKey, nil
}

func (m *Manager) checkNodeExited() {
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
		m.NodeExited(node.DeviceID)
	}
}

// NodeExited Node Exited
func (m *Manager) NodeExited(deviceID string) {
	err := persistent.GetDB().SetNodeExited(deviceID)
	if err != nil {
		log.Warnf("checkNodeExited SetNodeExited err:%s", err.Error())
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

	tk, err := m.authNew()
	if err != nil {
		log.Errorf("findDownloadinfoForBlocks AuthNew err:%s", err.Error())
		return reqList
	}

	for deviceID, list := range csMap {
		// info, err := persistent.GetDB().GetNodeAuthInfo(deviceID)

		node := m.GetCandidateNode(deviceID)
		if node != nil {
			reqList = append(reqList, api.ReqCacheData{BlockInfos: list, DownloadURL: node.Addr, DownloadToken: string(tk), CardFileHash: carfileHash, CacheID: cacheID})

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
