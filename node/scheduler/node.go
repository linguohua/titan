package scheduler

import (
	"fmt"
	"time"

	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/node/scheduler/db"
	"github.com/linguohua/titan/region"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/go-jsonrpc"
)

// EdgeNode Edge node
type EdgeNode struct {
	nodeAPI api.Edge
	closer  jsonrpc.ClientCloser

	deviceInfo api.DevicesInfo

	geoInfo region.GeoInfo

	addr string

	lastRequestTime time.Time
}

// CandidateNode Candidate node
type CandidateNode struct {
	nodeAPI api.Candidate
	closer  jsonrpc.ClientCloser

	deviceInfo api.DevicesInfo

	geoInfo region.GeoInfo

	addr string

	isValidator bool

	lastRequestTime time.Time
}

// NodeOnline Save DeciceInfo
func nodeOnline(deviceID string, onlineTime int64, geoInfo *region.GeoInfo, typeName api.NodeTypeName) error {
	oldNodeInfo, err := db.GetCacheDB().GetNodeInfo(deviceID)
	if err == nil {
		if typeName != oldNodeInfo.NodeType {
			return xerrors.New("node type inconsistent")
		}

		if oldNodeInfo.Geo != geoInfo.Geo {
			// delete old
			err = db.GetCacheDB().DelNodeWithGeoList(deviceID, oldNodeInfo.Geo)
			if err != nil {
				log.Errorf("DelNodeWithGeoList err:%v,deviceID:%v,Geo:%v", err.Error(), deviceID, oldNodeInfo.Geo)
			}
		}
	} else {
		if err.Error() != db.NotFind {
			log.Warnf("GetNodeInfo err:%v,deviceID:%v", err.Error(), deviceID)
		}
	}
	// log.Infof("oldgeo:%v,newgeo:%v,err:%v", nodeInfo.Geo, geoInfo.Geo, err)

	lastTime := time.Now().Format("2006-01-02 15:04:05")
	err = db.GetCacheDB().SetNodeInfo(deviceID, &db.NodeInfo{Geo: geoInfo.Geo, LastTime: lastTime, IsOnline: true, NodeType: typeName})
	if err != nil {
		return err
	}

	err = db.GetCacheDB().SetNodeToGeoList(deviceID, geoInfo.Geo)
	if err != nil {
		log.Errorf("SetNodeToGeoList err:%v,deviceID:%v,Geo:%v", err.Error(), deviceID, geoInfo.Geo)
		return err
	}

	// err = db.GetCacheDB().SetNodeToNodeList(deviceID, typeName)
	// if err != nil {
	// 	return err
	// }

	err = db.GetCacheDB().SetGeoToList(geoInfo.Geo)
	if err != nil {
		log.Errorf("SetGeoToList err:%v,Geo:%v", err.Error(), geoInfo.Geo)
		return err
	}

	// group
	// if typeName == api.TypeNameEdge {
	// 	edgeGrouping(deviceID, oldNodeInfo.Geo, geoInfo.Geo, bandwidth)
	// }

	return nil
}

// NodeOffline offline
func nodeOffline(deviceID string, geoInfo *region.GeoInfo, nodeType api.NodeTypeName, lastTime time.Time) error {
	err := db.GetCacheDB().DelNodeWithGeoList(deviceID, geoInfo.Geo)
	if err != nil {
		return err
	}

	// err = db.GetCacheDB().DelNodeWithNodeList(deviceID, nodeType)
	// if err != nil {
	// 	return err
	// }

	err = db.GetCacheDB().SetNodeInfo(deviceID, &db.NodeInfo{Geo: geoInfo.Geo, LastTime: lastTime.Format("2006-01-02 15:04:05"), IsOnline: false, NodeType: nodeType})
	if err != nil {
		return err
	}

	return nil
}

func updateLastRequestTime(deviceID string) {
	// log.Infof("updateLastRequestTime------------deviceID:%v", deviceID)
	lastTime := time.Now()

	edge := getEdgeNode(deviceID)
	if edge != nil {
		edge.lastRequestTime = lastTime
		return
	}

	candidate := getCandidateNode(deviceID)
	if candidate != nil {
		candidate.lastRequestTime = lastTime
		return
	}
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

	uInfo, err := region.GetRegion().GetGeoInfo(ip)
	if err != nil {
		log.Warnf("getNodeURLWithData GetGeoInfo err:%v,ip:%v", err, ip)
	}

	log.Infof("getNodeURLWithData user ip:%v,geo:%v,cid:%v", ip, uInfo.Geo, cid)

	var addr string
	nodeEs, geoLevelE := findEdgeNodeWithGeo(uInfo, deviceIDs)
	nodeCs, geoLevelC := findCandidateNodeWithGeo(uInfo, deviceIDs, []string{})
	if geoLevelE < geoLevelC {
		addr = nodeCs[randomNum(0, len(nodeCs))].deviceInfo.DownloadSrvURL
	} else if geoLevelE > geoLevelC {
		addr = nodeEs[randomNum(0, len(nodeEs))].deviceInfo.DownloadSrvURL
	} else {
		if len(nodeEs) > 0 {
			addr = nodeEs[randomNum(0, len(nodeEs))].deviceInfo.DownloadSrvURL
		} else {
			if len(nodeCs) > 0 {
				addr = nodeCs[randomNum(0, len(nodeCs))].deviceInfo.DownloadSrvURL
			} else {
				return "", xerrors.New("not find node")
			}
		}
	}

	// http://192.168.0.136:3456/rpc/v0/block/get?cid=QmeUqw4FY1wqnh2FMvuc2v8KAapE7fYwu2Up4qNwhZiRk7
	url := fmt.Sprintf("%s?cid=%s", addr, cid)

	return url, nil
}

// getCandidateNodesWithData find device
func getCandidateNodesWithData(cid string, geoInfo *region.GeoInfo) ([]*CandidateNode, error) {
	deviceIDs, err := db.GetCacheDB().GetNodesWithCacheList(cid)
	if err != nil {
		return nil, err
	}
	// log.Infof("getCandidateNodesWithData deviceIDs : %v", deviceIDs)

	if len(deviceIDs) <= 0 {
		return nil, xerrors.New("not find node ")
	}

	nodeCs, _ := findCandidateNodeWithGeo(geoInfo, deviceIDs, []string{})

	return nodeCs, nil
}
