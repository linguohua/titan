package scheduler

import (
	"context"
	"fmt"
	"math/rand"

	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/geoip"
	"github.com/linguohua/titan/node/scheduler/db"

	"golang.org/x/xerrors"
)

type geoLevel int64

const (
	defaultLevel  geoLevel = 0
	countryLevel  geoLevel = 1
	provinceLevel geoLevel = 2
	cityLevel     geoLevel = 3
)

// NotifyNodeCacheData Cache Data
func notifyNodeCacheData(cid, deviceID string) error {
	edge := getEdgeNode(deviceID)
	if edge == nil {
		return xerrors.New("node not find")
	}

	req := make([]api.ReqCacheData, 0)
	// TODO: generate ID
	reqData := api.ReqCacheData{Cid: cid, ID: "0"}
	req = append(req, reqData)

	err := edge.edgeAPI.CacheData(context.Background(), req)
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
		log.Errorf("GetNodeWithData GetGeoInfo err : %v ,ip : %v", err, ip)
	}

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

func randomNum(start, end int) int {
	// rand.Seed(time.Now().UnixNano())

	max := end - start
	x := rand.Intn(max)

	return start + x
}
