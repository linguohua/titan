package scheduler

import (
	"fmt"
	"sync"

	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/geoip"
	"github.com/linguohua/titan/node/scheduler/db"
	"golang.org/x/xerrors"
)

var (
	edgeNodeMap      sync.Map
	candidateNodeMap sync.Map

	edgeCount      int
	candidateCount int
	validatorCount int
)

func addEdgeNode(node *EdgeNode) {
	// geo ip
	geoInfo, err := geoip.GetGeoIP().GetGeoInfo(node.deviceInfo.ExternalIp)
	if err != nil {
		log.Errorf("addEdgeNode GetGeoInfo err:%v,node:%v", err, node.deviceInfo.ExternalIp)
	}

	node.geoInfo = geoInfo

	nodeOld := getEdgeNode(node.deviceInfo.DeviceId)
	if nodeOld != nil {
		nodeOld.closer()
		// log.Infof("close old deviceID:%v", nodeOld.deviceInfo.DeviceId)
	}

	log.Infof("addEdgeNode DeviceId:%v,geo:%v", node.deviceInfo.DeviceId, node.geoInfo.Geo)

	edgeNodeMap.Store(node.deviceInfo.DeviceId, node)

	err = nodeOnline(node.deviceInfo.DeviceId, 0, geoInfo, api.TypeNameEdge)
	if err != nil {
		log.Errorf("addEdgeNode NodeOnline err:%v", err)
	}

	edgeCount++
}

func getEdgeNode(deviceID string) *EdgeNode {
	nodeI, ok := edgeNodeMap.Load(deviceID)
	if ok && nodeI != nil {
		node := nodeI.(*EdgeNode)

		return node
	}

	return nil
}

func deleteEdgeNode(deviceID string) {
	node := getEdgeNode(deviceID)
	if node == nil {
		return
	}

	// close old node
	node.closer()

	edgeNodeMap.Delete(deviceID)

	err := nodeOffline(deviceID, node.geoInfo)
	if err != nil {
		log.Errorf("DeviceOffline err:%v", err)
	}

	edgeCount--
}

func addCandidateNode(node *CandidateNode) {
	node.isValidator, _ = db.GetCacheDB().IsNodeInValidatorList(node.deviceInfo.DeviceId)

	// geo ip
	geoInfo, err := geoip.GetGeoIP().GetGeoInfo(node.deviceInfo.ExternalIp)
	if err != nil {
		log.Errorf("addCandidateNode GetGeoInfo err:%v,ExternalIp:%v", err, node.deviceInfo.ExternalIp)
	}

	node.geoInfo = geoInfo

	nodeOld := getCandidateNode(node.deviceInfo.DeviceId)
	if nodeOld != nil {
		nodeOld.closer()
		// log.Infof("close old deviceID:%v", nodeOld.deviceInfo.DeviceId)
	}

	candidateNodeMap.Store(node.deviceInfo.DeviceId, node)

	log.Infof("addCandidateNode DeviceId:%v,geo:%v", node.deviceInfo.DeviceId, node.geoInfo.Geo)

	err = nodeOnline(node.deviceInfo.DeviceId, 0, geoInfo, api.TypeNameCandidate)
	if err != nil {
		log.Errorf("addCandidateNode NodeOnline err:%v", err)
	}

	if node.isValidator {
		validatorCount++
	} else {
		candidateCount++
	}
}

func getCandidateNode(deviceID string) *CandidateNode {
	nodeI, ok := candidateNodeMap.Load(deviceID)
	if ok && nodeI != nil {
		node := nodeI.(*CandidateNode)

		return node
	}

	return nil
}

func deleteCandidateNode(deviceID string) {
	node := getCandidateNode(deviceID)
	if node == nil {
		return
	}

	// close old node
	node.closer()

	candidateNodeMap.Delete(deviceID)

	err := nodeOffline(deviceID, node.geoInfo)
	if err != nil {
		log.Errorf("DeviceOffline err:%v", err)
	}

	if node.isValidator {
		validatorCount--
	} else {
		candidateCount--
	}
}

func findEdgeNodeWithGeo(userGeoInfo geoip.GeoInfo, deviceIDs []string) ([]*EdgeNode, geoLevel) {
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

func findCandidateNodeWithGeo(userGeoInfo geoip.GeoInfo, deviceIDs []string) ([]*CandidateNode, geoLevel) {
	sameCountryNodes := make([]*CandidateNode, 0)
	sameProvinceNodes := make([]*CandidateNode, 0)
	sameCityNodes := make([]*CandidateNode, 0)

	defaultNodes := make([]*CandidateNode, 0)

	if len(deviceIDs) > 0 {
		for _, dID := range deviceIDs {
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

// getNodeURLWithData find device
func getNodeURLWithData(cid, ip string) (string, error) {
	deviceIDs, err := db.GetCacheDB().GetNodesWithCacheList(cid)
	if err != nil {
		return "", err
	}

	if len(deviceIDs) <= 0 {
		return "", xerrors.New("not find node")
	}

	uInfo, err := geoip.GetGeoIP().GetGeoInfo(ip)
	if err != nil {
		log.Errorf("getNodeURLWithData GetGeoInfo err:%v,ip:%v", err, ip)
	}

	log.Infof("getNodeURLWithData user ip:%v,geo:%v,cid:%v", ip, uInfo.Geo, cid)

	var addr string
	nodeEs, geoLevelE := findEdgeNodeWithGeo(uInfo, deviceIDs)
	nodeCs, geoLevelC := findCandidateNodeWithGeo(uInfo, deviceIDs)
	if geoLevelE < geoLevelC {
		addr = nodeCs[randomNum(0, len(nodeCs)-1)].addr
	} else if geoLevelE > geoLevelC {
		addr = nodeEs[randomNum(0, len(nodeEs)-1)].addr
	} else {
		if len(nodeEs) > 0 {
			addr = nodeEs[randomNum(0, len(nodeEs)-1)].addr
		} else {
			if len(nodeCs) > 0 {
				addr = nodeCs[randomNum(0, len(nodeCs)-1)].addr
			} else {
				return "", xerrors.New("not find node")
			}
		}
	}

	// http://192.168.0.136:3456/rpc/v0/block/get?cid=QmeUqw4FY1wqnh2FMvuc2v8KAapE7fYwu2Up4qNwhZiRk7
	url := fmt.Sprintf("%s/block/get?cid=%s", addr, cid)

	return url, nil
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
