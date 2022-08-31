package scheduler

import (
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

	Node
}

// CandidateNode Candidate node
type CandidateNode struct {
	nodeAPI     api.Candidate
	closer      jsonrpc.ClientCloser
	isValidator bool

	Node
}

// Node Common
type Node struct {
	deviceInfo api.DevicesInfo

	geoInfo region.GeoInfo

	addr string

	lastRequestTime time.Time
}

// NodeOnline Save DeciceInfo
func (n *Node) nodeOnline(deviceID string, onlineTime int64, geoInfo *region.GeoInfo, typeName api.NodeTypeName) error {
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
func (n *Node) nodeOffline(deviceID string, geoInfo *region.GeoInfo, nodeType api.NodeTypeName, lastTime time.Time) error {
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
