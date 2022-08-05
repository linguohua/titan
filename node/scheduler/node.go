package scheduler

import (
	"time"

	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/geoip"
	"github.com/linguohua/titan/node/scheduler/db"

	"github.com/filecoin-project/go-jsonrpc"
)

// EdgeNode Edge node
type EdgeNode struct {
	edgeAPI api.Edge
	closer  jsonrpc.ClientCloser

	deviceInfo api.DevicesInfo

	geoInfo geoip.GeoInfo

	addr string
}

// CandidateNode Candidate node
type CandidateNode struct {
	edgeAPI api.Candidate
	closer  jsonrpc.ClientCloser

	deviceInfo api.DevicesInfo

	geoInfo geoip.GeoInfo

	addr string
}

// NodeOnline Save DeciceInfo
func nodeOnline(deviceID string, onlineTime int64, geoInfo geoip.GeoInfo, typeName api.NodeTypeName) error {
	nodeInfo, err := db.GetCacheDB().GetNodeInfo(deviceID)
	if err == nil && nodeInfo.Geo != geoInfo.Geo {
		// delete old
		err = db.GetCacheDB().DelNodeWithGeoList(deviceID, nodeInfo.Geo)
		log.Infof("SremSet err:%v", err)
	}
	log.Infof("oldgeo:%v,newgeo:%v,err:%v", nodeInfo.Geo, geoInfo.Geo, err)

	lastTime := time.Now().Format("2006-01-02 15:04:05")
	err = db.GetCacheDB().SetNodeInfo(deviceID, db.NodeInfo{OnLineTime: onlineTime, Geo: geoInfo.Geo, LastTime: lastTime, IsOnline: true})
	if err != nil {
		return err
	}

	err = db.GetCacheDB().SetNodeToGeoList(deviceID, geoInfo.Geo)
	if err != nil {
		return err
	}

	err = db.GetCacheDB().SetNodeToNodeList(deviceID, typeName)
	if err != nil {
		return err
	}

	return nil
}

// NodeOffline offline
func nodeOffline(deviceID string, geoInfo geoip.GeoInfo) error {
	err := db.GetCacheDB().DelNodeWithGeoList(deviceID, geoInfo.Geo)
	if err != nil {
		return err
	}

	lastTime := time.Now().Format("2006-01-02 15:04:05")
	err = db.GetCacheDB().SetNodeInfo(deviceID, db.NodeInfo{OnLineTime: 0, Geo: geoInfo.Geo, LastTime: lastTime, IsOnline: false})
	if err != nil {
		return err
	}

	return nil
}

func spotCheck() {
	// find edge

	// find validator
}

// 选举
func election() {
	// 每个城市 选出X个验证者
	// 每隔Y时间 重新选举
	//
}
