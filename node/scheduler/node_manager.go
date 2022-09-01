package scheduler

import (
	"fmt"
	"sync"
	"time"

	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/node/scheduler/db"
	"github.com/linguohua/titan/region"
	"github.com/ouqiang/timewheel"
	"golang.org/x/xerrors"
)

// 节点管理器 管理节点对象
// 边缘节点分组 为了方便抽查
// 候选节点选举
// 候选节点与边缘节点组 匹配 (抽查)

type geoLevel int64

const (
	defaultLevel  geoLevel = 0
	countryLevel  geoLevel = 1
	provinceLevel geoLevel = 2
	cityLevel     geoLevel = 3

	dataDefaultTag string = "-1"
)

// NodeManager Node Manager
type NodeManager struct {
	edgeNodeMap      sync.Map
	candidateNodeMap sync.Map

	edgeCount      int
	candidateCount int
	validatorCount int

	timewheelKeepalive *timewheel.TimeWheel
	keepaliveTime      int // 保活时间间隔 (分钟)
}

func newNodeManager() *NodeManager {
	nodeManager := &NodeManager{keepaliveTime: 1}

	nodeManager.initKeepaliveTimewheel()

	return nodeManager
}

// GetNodeManager Get NodeManager
// func GetNodeManager() *NodeManager {
// 	return nodeManager
// }

// InitKeepaliveTimewheel ndoe Keepalive
func (m *NodeManager) initKeepaliveTimewheel() {
	// 保活定时器
	m.timewheelKeepalive = timewheel.New(1*time.Second, 3600, func(_ interface{}) {
		m.nodeKeepalive()
		// 继续添加定时器
		m.timewheelKeepalive.AddTimer(time.Duration(1)*60*time.Second, "Keepalive", nil)
	})
	m.timewheelKeepalive.Start()
	// 开始一个事件处理
	m.timewheelKeepalive.AddTimer(time.Duration(1)*60*time.Second, "Keepalive", nil)
}

func (m *NodeManager) nodeKeepalive() {
	nowTime := time.Now().Add(-time.Duration(m.keepaliveTime) * 60 * time.Second)

	// log.Warnf("nodeKeepalive nowTime:%v, time.Now():%v", nowTime, time.Now())

	m.edgeNodeMap.Range(func(key, value interface{}) bool {
		deviceID := key.(string)
		node := value.(*EdgeNode)

		if node == nil {
			return true
		}

		lastTime := node.lastRequestTime

		if !lastTime.After(nowTime) {
			// 离线
			m.deleteEdgeNode(node)
			node = nil
			return true
		}

		err := db.GetCacheDB().AddNodeOnlineTime(deviceID, int64(m.keepaliveTime))
		if err != nil {
			log.Warnf("AddNodeOnlineTime err:%v,deviceID:%v", err.Error(), deviceID)
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
			// 离线
			m.deleteCandidateNode(node)
			node = nil
			return true
		}

		err := db.GetCacheDB().AddNodeOnlineTime(deviceID, int64(m.keepaliveTime))
		if err != nil {
			log.Warnf("AddNodeOnlineTime err:%v,deviceID:%v", err.Error(), deviceID)
		}

		return true
	})
}

func (m *NodeManager) addEdgeNode(node *EdgeNode) error {
	deviceID := node.deviceInfo.DeviceId
	// geo ip
	geoInfo, err := region.GetRegion().GetGeoInfo(node.deviceInfo.ExternalIp)
	if err != nil {
		log.Warnf("addEdgeNode GetGeoInfo err:%v,node:%v", err, node.deviceInfo.ExternalIp)
	}

	node.geoInfo = *geoInfo

	nodeOld := m.getEdgeNode(deviceID)
	if nodeOld != nil {
		nodeOld.closer()

		nodeOld = nil
		// log.Infof("close old deviceID:%v", nodeOld.deviceInfo.DeviceId)
	}

	log.Infof("addEdgeNode DeviceId:%v,geo:%v", deviceID, node.geoInfo.Geo)

	err = node.nodeOnline(deviceID, 0, geoInfo, api.TypeNameEdge)
	if err != nil {
		// log.Errorf("addEdgeNode NodeOnline err:%v", err)
		return err
	}

	m.edgeNodeMap.Store(deviceID, node)

	m.edgeCount++

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

func (m *NodeManager) deleteEdgeNode(node *EdgeNode) {
	deviceID := node.deviceInfo.DeviceId
	// close old node
	node.closer()

	log.Warnf("deleteEdgeNode :%v", deviceID)

	m.edgeNodeMap.Delete(deviceID)

	err := node.nodeOffline(deviceID, &node.geoInfo, api.TypeNameEdge, node.lastRequestTime)
	if err != nil {
		log.Errorf("DeviceOffline err:%v,deviceID:%v", err.Error(), deviceID)
	}

	m.edgeCount--
}

func (m *NodeManager) addCandidateNode(node *CandidateNode) error {
	deviceID := node.deviceInfo.DeviceId

	node.isValidator, _ = db.GetCacheDB().IsNodeInValidatorList(deviceID)

	// geo ip
	geoInfo, err := region.GetRegion().GetGeoInfo(node.deviceInfo.ExternalIp)
	if err != nil {
		log.Warnf("addCandidateNode GetGeoInfo err:%v,ExternalIp:%v", err, node.deviceInfo.ExternalIp)
	}

	node.geoInfo = *geoInfo

	nodeOld := m.getCandidateNode(deviceID)
	if nodeOld != nil {
		nodeOld.closer()

		nodeOld = nil
		// log.Infof("close old deviceID:%v", nodeOld.deviceInfo.DeviceId)
	}

	log.Infof("addCandidateNode DeviceId:%v,geo:%v", deviceID, node.geoInfo.Geo)

	err = node.nodeOnline(deviceID, 0, geoInfo, api.TypeNameCandidate)
	if err != nil {
		// log.Errorf("addCandidateNode NodeOnline err:%v", err)
		return err
	}

	m.candidateNodeMap.Store(deviceID, node)

	if node.isValidator {
		m.validatorCount++
	} else {
		m.candidateCount++
	}

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

func (m *NodeManager) deleteCandidateNode(node *CandidateNode) {
	deviceID := node.deviceInfo.DeviceId
	// close old node
	node.closer()

	log.Warnf("deleteCandidateNode :%v", deviceID)

	m.candidateNodeMap.Delete(deviceID)

	err := node.nodeOffline(deviceID, &node.geoInfo, api.TypeNameCandidate, node.lastRequestTime)
	if err != nil {
		log.Errorf("DeviceOffline err:%v,deviceID:%v", err.Error(), deviceID)
	}

	if node.isValidator {
		m.validatorCount--
	} else {
		m.candidateCount--
	}
}

func (m *NodeManager) findEdgeNodeWithGeo(userGeoInfo *region.GeoInfo, deviceIDs []string) ([]*EdgeNode, geoLevel) {
	sameCountryNodes := make([]*EdgeNode, 0)
	sameProvinceNodes := make([]*EdgeNode, 0)
	sameCityNodes := make([]*EdgeNode, 0)

	defaultNodes := make([]*EdgeNode, 0)

	for _, dID := range deviceIDs {
		node := m.getEdgeNode(dID)
		if node == nil {
			continue
		}

		defaultNodes = append(defaultNodes, node)

		if node.geoInfo.Country == userGeoInfo.Country {
			sameCountryNodes = append(sameCountryNodes, node)

			if node.geoInfo.Province == userGeoInfo.Province {
				sameProvinceNodes = append(sameProvinceNodes, node)

				if node.geoInfo.City == userGeoInfo.City {
					sameCityNodes = append(sameCityNodes, node)
				}
			}
		}
	}

	if len(sameCityNodes) > 0 {
		return sameCityNodes, cityLevel
	}

	if len(sameProvinceNodes) > 0 {
		return sameProvinceNodes, provinceLevel
	}

	if len(sameCountryNodes) > 0 {
		return sameCountryNodes, countryLevel
	}

	return defaultNodes, defaultLevel
}

func (m *NodeManager) findCandidateNodeWithGeo(userGeoInfo *region.GeoInfo, useDeviceIDs, filterDeviceIDs []string) ([]*CandidateNode, geoLevel) {
	sameCountryNodes := make([]*CandidateNode, 0)
	sameProvinceNodes := make([]*CandidateNode, 0)
	sameCityNodes := make([]*CandidateNode, 0)

	defaultNodes := make([]*CandidateNode, 0)

	if len(useDeviceIDs) > 0 {
		for _, dID := range useDeviceIDs {
			node := m.getCandidateNode(dID)
			if node == nil {
				continue
			}

			defaultNodes = append(defaultNodes, node)

			if node.geoInfo.Country == userGeoInfo.Country {
				sameCountryNodes = append(sameCountryNodes, node)

				if node.geoInfo.Province == userGeoInfo.Province {
					sameProvinceNodes = append(sameProvinceNodes, node)

					if node.geoInfo.City == userGeoInfo.City {
						sameCityNodes = append(sameCityNodes, node)
					}
				}
			}
		}
	} else {
		m.candidateNodeMap.Range(func(key, value interface{}) bool {
			node := value.(*CandidateNode)

			defaultNodes = append(defaultNodes, node)

			if node.geoInfo.Country == userGeoInfo.Country {
				sameCountryNodes = append(sameCountryNodes, node)

				if node.geoInfo.Province == userGeoInfo.Province {
					sameProvinceNodes = append(sameProvinceNodes, node)

					if node.geoInfo.City == userGeoInfo.City {
						sameCityNodes = append(sameCityNodes, node)
					}
				}
			}
			return true
		})
	}

	if len(sameCityNodes) > 0 {
		if len(filterDeviceIDs) > 0 {
			sameCityNodes2 := m.filterCandidates(filterDeviceIDs, sameCityNodes)
			if len(sameCityNodes2) > 0 {
				return sameCityNodes2, cityLevel
			}
		} else {
			return sameCityNodes, cityLevel
		}
	}

	if len(sameProvinceNodes) > 0 {
		if len(filterDeviceIDs) > 0 {
			sameProvinceNodes2 := m.filterCandidates(filterDeviceIDs, sameProvinceNodes)
			if len(sameProvinceNodes2) > 0 {
				return sameProvinceNodes2, provinceLevel
			}
		} else {
			return sameProvinceNodes, provinceLevel
		}
	}

	if len(sameCountryNodes) > 0 {
		if len(filterDeviceIDs) > 0 {
			sameCountryNodes2 := m.filterCandidates(filterDeviceIDs, sameCountryNodes)
			if len(sameCountryNodes2) > 0 {
				return sameCountryNodes2, countryLevel
			}
		} else {
			return sameCountryNodes, countryLevel
		}
	}

	if len(filterDeviceIDs) > 0 {
		defaultNodes2 := m.filterCandidates(filterDeviceIDs, defaultNodes)
		return defaultNodes2, defaultLevel
	}
	return defaultNodes, defaultLevel
}

func (m *NodeManager) filterCandidates(filterDeviceIDs []string, sameNodes []*CandidateNode) []*CandidateNode {
	sameNodes2 := make([]*CandidateNode, 0)
	for _, node := range sameNodes {
		isHave := false
		for _, nd := range filterDeviceIDs {
			if node.deviceInfo.DeviceId == nd {
				isHave = true
			}
		}

		if !isHave {
			sameNodes2 = append(sameNodes2, node)
		}
	}

	return sameNodes2
}

func (m *NodeManager) resetCandidateAndValidatorCount() {
	m.candidateCount = 0
	m.validatorCount = 0

	m.candidateNodeMap.Range(func(key, value interface{}) bool {
		node := value.(*CandidateNode)

		if node.isValidator {
			m.validatorCount++
		} else {
			m.candidateCount++
		}

		return true
	})
}

func (m *NodeManager) updateLastRequestTime(deviceID string) {
	// log.Infof("updateLastRequestTime------------deviceID:%v", deviceID)
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
func (m *NodeManager) getNodeURLWithData(cid, ip string) (string, error) {
	deviceIDs, err := db.GetCacheDB().GetNodesWithCacheList(cid)
	if err != nil {
		return "", err
	}

	if len(deviceIDs) <= 0 {
		return "", xerrors.New("not find node")
	}

	uInfo, err := region.GetRegion().GetGeoInfo(ip)
	if err != nil {
		log.Warnf("getNodeURLWithData GetGeoInfo err:%v,ip:%v", err, ip)
	}

	log.Infof("getNodeURLWithData user ip:%v,geo:%v,cid:%v", ip, uInfo.Geo, cid)

	var url string
	nodeEs, geoLevelE := m.findEdgeNodeWithGeo(uInfo, deviceIDs)
	nodeCs, geoLevelC := m.findCandidateNodeWithGeo(uInfo, deviceIDs, []string{})
	if geoLevelE < geoLevelC {
		url = nodeCs[randomNum(0, len(nodeCs))].deviceInfo.DownloadSrvURL
	} else if geoLevelE > geoLevelC {
		url = nodeEs[randomNum(0, len(nodeEs))].deviceInfo.DownloadSrvURL
	} else {
		if len(nodeEs) > 0 {
			url = nodeEs[randomNum(0, len(nodeEs))].deviceInfo.DownloadSrvURL
		} else {
			if len(nodeCs) > 0 {
				url = nodeCs[randomNum(0, len(nodeCs))].deviceInfo.DownloadSrvURL
			} else {
				return "", xerrors.New("not find node")
			}
		}
	}

	// http://192.168.0.136:3456/rpc/v0/block/get?cid=QmeUqw4FY1wqnh2FMvuc2v8KAapE7fYwu2Up4qNwhZiRk7
	return fmt.Sprintf("%s?cid=%s", url, cid), nil
}

// getCandidateNodesWithData find device
func (m *NodeManager) getCandidateNodesWithData(cid string, geoInfo *region.GeoInfo) ([]*CandidateNode, error) {
	deviceIDs, err := db.GetCacheDB().GetNodesWithCacheList(cid)
	if err != nil {
		return nil, err
	}
	// log.Infof("getCandidateNodesWithData deviceIDs : %v", deviceIDs)

	if len(deviceIDs) <= 0 {
		return nil, xerrors.New("not find node ")
	}

	nodeCs, _ := m.findCandidateNodeWithGeo(geoInfo, deviceIDs, []string{})

	return nodeCs, nil
}
