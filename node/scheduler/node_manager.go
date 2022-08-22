package scheduler

import (
	"sync"
	"time"

	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/node/scheduler/db"
	"github.com/linguohua/titan/region"
	"github.com/ouqiang/timewheel"
)

// 节点管理器 管理节点对象
// 边缘节点分组 为了方便抽查
// 候选节点选举
// 候选节点与边缘节点组 匹配 (抽查)

var (
	edgeNodeMap      sync.Map
	candidateNodeMap sync.Map

	edgeCount      int
	candidateCount int
	validatorCount int

	timewheelKeepalive *timewheel.TimeWheel
	keepaliveTime      = 1 // 保活时间间隔 (分钟)
)

// InitKeepaliveTimewheel ndoe Keepalive
func InitKeepaliveTimewheel() {
	// 保活定时器
	timewheelKeepalive = timewheel.New(1*time.Second, 3600, func(_ interface{}) {
		nodeKeepalive()
		// 继续添加定时器
		timewheelKeepalive.AddTimer(time.Duration(1)*60*time.Second, "Keepalive", nil)
	})
	timewheelKeepalive.Start()
	// 开始一个事件处理
	timewheelKeepalive.AddTimer(time.Duration(1)*60*time.Second, "Keepalive", nil)
}

func nodeKeepalive() {
	nowTime := time.Now().Add(-time.Duration(keepaliveTime) * 60 * time.Second)

	// log.Warnf("nodeKeepalive nowTime:%v, time.Now():%v", nowTime, time.Now())

	edgeNodeMap.Range(func(key, value interface{}) bool {
		// deviceID := key.(string)
		node := value.(*EdgeNode)

		if node == nil {
			return true
		}

		lastTime := node.lastRequestTime
		// log.Warnf("nodeKeepalive deviceID:%v ,lastTime:%v,After:%v", deviceID, lastTime, !lastTime.After(nowTime))

		if !lastTime.After(nowTime) {
			// 离线
			deleteEdgeNode(node, lastTime)
		}

		return true
	})

	candidateNodeMap.Range(func(key, value interface{}) bool {
		// deviceID := key.(string)
		node := value.(*CandidateNode)

		if node == nil {
			return true
		}

		lastTime := node.lastRequestTime

		if !lastTime.After(nowTime) {
			// 离线
			deleteCandidateNode(node, lastTime)
		}

		return true
	})
}

func addEdgeNode(node *EdgeNode) error {
	// geo ip
	geoInfo, err := region.GetRegion().GetGeoInfo(node.deviceInfo.ExternalIp)
	if err != nil {
		log.Warnf("addEdgeNode GetGeoInfo err:%v,node:%v", err, node.deviceInfo.ExternalIp)
	}

	node.geoInfo = geoInfo

	nodeOld := getEdgeNode(node.deviceInfo.DeviceId)
	if nodeOld != nil {
		nodeOld.closer()
		// log.Infof("close old deviceID:%v", nodeOld.deviceInfo.DeviceId)
	}

	log.Infof("addEdgeNode DeviceId:%v,geo:%v", node.deviceInfo.DeviceId, node.geoInfo.Geo)

	err = nodeOnline(node.deviceInfo.DeviceId, 0, geoInfo, api.TypeNameEdge, node.bandwidth)
	if err != nil {
		// log.Errorf("addEdgeNode NodeOnline err:%v", err)
		return err
	}

	edgeNodeMap.Store(node.deviceInfo.DeviceId, node)

	edgeCount++

	return nil
}

func getEdgeNode(deviceID string) *EdgeNode {
	nodeI, ok := edgeNodeMap.Load(deviceID)
	if ok && nodeI != nil {
		node := nodeI.(*EdgeNode)

		return node
	}

	return nil
}

func deleteEdgeNode(node *EdgeNode, lastTime time.Time) {
	deviceID := node.deviceInfo.DeviceId
	// close old node
	node.closer()

	edgeNodeMap.Delete(deviceID)

	err := nodeOffline(deviceID, node.geoInfo, api.TypeNameEdge, lastTime)
	if err != nil {
		log.Errorf("DeviceOffline err:%v,deviceID:%v", err.Error(), deviceID)
	}

	edgeCount--
}

func addCandidateNode(node *CandidateNode) error {
	node.isValidator, _ = db.GetCacheDB().IsNodeInValidatorList(node.deviceInfo.DeviceId)

	// geo ip
	geoInfo, err := region.GetRegion().GetGeoInfo(node.deviceInfo.ExternalIp)
	if err != nil {
		log.Warnf("addCandidateNode GetGeoInfo err:%v,ExternalIp:%v", err, node.deviceInfo.ExternalIp)
	}

	node.geoInfo = geoInfo

	nodeOld := getCandidateNode(node.deviceInfo.DeviceId)
	if nodeOld != nil {
		nodeOld.closer()
		// log.Infof("close old deviceID:%v", nodeOld.deviceInfo.DeviceId)
	}

	log.Infof("addCandidateNode DeviceId:%v,geo:%v", node.deviceInfo.DeviceId, node.geoInfo.Geo)

	err = nodeOnline(node.deviceInfo.DeviceId, 0, geoInfo, api.TypeNameCandidate, node.bandwidth)
	if err != nil {
		// log.Errorf("addCandidateNode NodeOnline err:%v", err)
		return err
	}

	candidateNodeMap.Store(node.deviceInfo.DeviceId, node)

	if node.isValidator {
		validatorCount++
	} else {
		candidateCount++
	}

	return nil
}

func getCandidateNode(deviceID string) *CandidateNode {
	nodeI, ok := candidateNodeMap.Load(deviceID)
	if ok && nodeI != nil {
		node := nodeI.(*CandidateNode)

		return node
	}

	return nil
}

func deleteCandidateNode(node *CandidateNode, lastTime time.Time) {
	deviceID := node.deviceInfo.DeviceId
	// close old node
	node.closer()

	candidateNodeMap.Delete(deviceID)

	err := nodeOffline(deviceID, node.geoInfo, api.TypeNameCandidate, lastTime)
	if err != nil {
		log.Errorf("DeviceOffline err:%v,deviceID:%v", err.Error(), deviceID)
	}

	if node.isValidator {
		validatorCount--
	} else {
		candidateCount--
	}
}

func findEdgeNodeWithGeo(userGeoInfo region.GeoInfo, deviceIDs []string) ([]*EdgeNode, geoLevel) {
	sameCountryNodes := make([]*EdgeNode, 0)
	sameProvinceNodes := make([]*EdgeNode, 0)
	sameCityNodes := make([]*EdgeNode, 0)

	defaultNodes := make([]*EdgeNode, 0)

	for _, dID := range deviceIDs {
		node := getEdgeNode(dID)
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

func findCandidateNodeWithGeo(userGeoInfo region.GeoInfo, useDeviceIDs, filterDeviceIDs []string) ([]*CandidateNode, geoLevel) {
	sameCountryNodes := make([]*CandidateNode, 0)
	sameProvinceNodes := make([]*CandidateNode, 0)
	sameCityNodes := make([]*CandidateNode, 0)

	defaultNodes := make([]*CandidateNode, 0)

	if len(useDeviceIDs) > 0 {
		for _, dID := range useDeviceIDs {
			node := getCandidateNode(dID)
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
		candidateNodeMap.Range(func(key, value interface{}) bool {
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
			sameCityNodes2 := filterCandidates(filterDeviceIDs, sameCityNodes)
			if len(sameCityNodes2) > 0 {
				return sameCityNodes2, cityLevel
			}
		} else {
			return sameCityNodes, cityLevel
		}
	}

	if len(sameProvinceNodes) > 0 {
		if len(filterDeviceIDs) > 0 {
			sameProvinceNodes2 := filterCandidates(filterDeviceIDs, sameProvinceNodes)
			if len(sameProvinceNodes2) > 0 {
				return sameProvinceNodes2, provinceLevel
			}
		} else {
			return sameProvinceNodes, provinceLevel
		}
	}

	if len(sameCountryNodes) > 0 {
		if len(filterDeviceIDs) > 0 {
			sameCountryNodes2 := filterCandidates(filterDeviceIDs, sameCountryNodes)
			if len(sameCountryNodes2) > 0 {
				return sameCountryNodes2, countryLevel
			}
		} else {
			return sameCountryNodes, countryLevel
		}
	}

	if len(filterDeviceIDs) > 0 {
		defaultNodes2 := filterCandidates(filterDeviceIDs, defaultNodes)
		return defaultNodes2, defaultLevel
	}
	return defaultNodes, defaultLevel
}

func filterCandidates(filterDeviceIDs []string, sameNodes []*CandidateNode) []*CandidateNode {
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

func resetCandidateAndValidatorCount() {
	candidateCount = 0
	validatorCount = 0

	candidateNodeMap.Range(func(key, value interface{}) bool {
		node := value.(*CandidateNode)

		if node.isValidator {
			validatorCount++
		} else {
			candidateCount++
		}

		return true
	})
}
