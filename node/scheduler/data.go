package scheduler

import (
	"context"

	"github.com/linguohua/titan/geoip"
	"github.com/linguohua/titan/node/scheduler/db"

	"golang.org/x/xerrors"
)

// NotifyNodeCacheData Cache Data
func notifyNodeCacheData(cid, deviceID string) error {
	edge := getEdgeNode(deviceID)
	if edge == nil {
		return xerrors.New("node not find")
	}

	err := edge.edgeAPI.CacheData(context.Background(), []string{cid})
	if err != nil {
		log.Errorf("NotifyNodeCacheData err : %v", err)
		return err
	}

	err = nodeCacheReady(deviceID, cid)
	if err != nil {
		log.Errorf("NotifyNodeCacheData nodeCacheReady err : %v", err)
		return err
	}

	return nil
}

// NodeCacheResult Device Cache Result
func nodeCacheResult(deviceID, cid string, isOk bool) error {
	log.Infof("nodeCacheResult deviceID:%v,cid:%v,isOk:%v", deviceID, cid, isOk)
	if !isOk {
		return db.GetCacheDB().DelCacheDataInfo(deviceID, cid)
	}

	return db.GetCacheDB().SetNodeToCacheList(deviceID, cid)
}

// Node Cache ready
func nodeCacheReady(deviceID, cid string) error {
	v, err := db.GetCacheDB().GetCacheDataInfo(deviceID, cid)
	if err == nil && v != "" {
		return xerrors.Errorf("already cache")
	}

	tag, err := db.GetCacheDB().GetNodeCacheTag(deviceID)
	if err != nil {
		log.Errorf("NotifyNodeCacheData getTagWithNode err : %v", err)
		return err
	}

	return db.GetCacheDB().SetCacheDataInfo(deviceID, cid, tag)
}

// GetNodeWithData find device
func GetNodeWithData(cid, ip string) (*EdgeNode, error) {
	deviceIDs, err := db.GetCacheDB().GetNodesWithCacheList(cid)
	if err != nil {
		return nil, err
	}

	if len(deviceIDs) <= 0 {
		return nil, xerrors.New("not find node")
	}

	uInfo, err := geoip.GetGeoIP().GetGeoInfo(ip)
	if err != nil {
		log.Errorf("GetNodeWithData GetGeoInfo err : %v ,ip : %v", err, ip)
	}

	node := findNodeWithGeo(uInfo, deviceIDs)
	if node == nil {
		return nil, xerrors.New("not find node")
	}

	return node, nil
}

func findNodeWithGeo(userGeoInfo geoip.GeoInfo, deviceIDs []string) *EdgeNode {
	sameCountryNodes := make([]*EdgeNode, 0)
	sameProvinceNodes := make([]*EdgeNode, 0)
	sameCityNodes := make([]*EdgeNode, 0)

	var defaultNode *EdgeNode

	for _, dID := range deviceIDs {
		node := getEdgeNode(dID)
		if node == nil {
			continue
		}

		defaultNode = node

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
		return sameCityNodes[0]
	}

	if len(sameProvinceNodes) > 0 {
		return sameProvinceNodes[0]
	}

	if len(sameCountryNodes) > 0 {
		return sameCountryNodes[0]
	}

	return defaultNode
}
