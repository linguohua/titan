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

	deviceID string
	addr     string
	userID   string
	ip       string

	geoInfo geoip.GeoInfo
}

// CandidateNode Candidate node
type CandidateNode struct {
	edgeAPI api.Candidate
	closer  jsonrpc.ClientCloser

	deviceID string
	addr     string
	userID   string
	ip       string

	geoInfo geoip.GeoInfo
}

// NodeOnline Save DeciceInfo
func nodeOnline(deviceID string, onlineTime int64, geoInfo geoip.GeoInfo) error {
	nodeInfo, err := db.GetCacheDB().GetNodeInfo(deviceID)
	if err == nil && nodeInfo.Geo != geoInfo.Geo {
		// delete old
		err = db.GetCacheDB().DelNodeWithGeoList(deviceID, nodeInfo.Geo)
		// log.Infof("SremSet err:%v", err)
	}
	// log.Infof("oldgeo:%v,newgeo:%v,err:%v", nodeInfo.Geo, geoInfo.Geo, err)

	err = db.GetCacheDB().SetNodeToGeoList(deviceID, geoInfo.Geo)
	if err != nil {
		return err
	}

	lastTime := time.Now().Format("2006-01-02 15:04:05")
	err = db.GetCacheDB().SetNodeInfo(deviceID, db.NodeInfo{OnLineTime: onlineTime, Geo: geoInfo.Geo, LastTime: lastTime})
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

	// 	keyN := fmt.Sprintf(db.RedisKeyNodeInfo, deviceID)
	// 	err = db.GetCacheDB().HSetValue(keyN, isOnLineField, false)
	// 	if err != nil {
	// 		return err
	// 	}

	return nil
}
