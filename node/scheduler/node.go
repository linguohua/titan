package scheduler

import (
	"fmt"
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

const (
	// redis field
	lastTimeField   = "LastTime"
	onLineTimeField = "OnLineTime"
	isOnLineField   = "IsOnLine"
	isoCodeField    = "IsoCodeField"
)

// NodeOnline Save DeciceInfo
func NodeOnline(deviceID string, onlineTime int64, geoInfo geoip.GeoInfo) error {
	keyN := fmt.Sprintf(db.RedisKeyNodeInfo, deviceID)

	oldIsoCode, err := db.GetCacheDB().HGetValue(keyN, isoCodeField)
	log.Infof("oldiso:%v,newiso:%v,err:%v", oldIsoCode, geoInfo.IsoCode, err)
	if err == nil && oldIsoCode != geoInfo.IsoCode {
		// delete old
		keyG := fmt.Sprintf(db.RedisKeyGeoNodeList, oldIsoCode)
		err = db.GetCacheDB().SremSet(keyG, deviceID)
		log.Infof("SremSet err:%v", err)
	}

	keyG := fmt.Sprintf(db.RedisKeyGeoNodeList, geoInfo.IsoCode)
	err = db.GetCacheDB().AddSet(keyG, deviceID)
	if err != nil {
		return err
	}

	lastTime := time.Now().Format("2006-01-02 15:04:05")
	err = db.GetCacheDB().HSetValues(keyN, lastTimeField, lastTime, isOnLineField, true, isoCodeField, geoInfo.IsoCode)
	if err != nil {
		return err
	}

	err = db.GetCacheDB().IncrbyField(keyN, onLineTimeField, onlineTime)
	if err != nil {
		return err
	}

	return nil
}

// NodeOffline offline
func NodeOffline(deviceID string, geoInfo geoip.GeoInfo) error {
	keyG := fmt.Sprintf(db.RedisKeyGeoNodeList, deviceID)
	err := db.GetCacheDB().SremSet(keyG, geoInfo.IsoCode)
	if err != nil {
		return err
	}

	keyN := fmt.Sprintf(db.RedisKeyNodeInfo, deviceID)
	err = db.GetCacheDB().HSetValue(keyN, isOnLineField, false)
	if err != nil {
		return err
	}

	return nil
}
