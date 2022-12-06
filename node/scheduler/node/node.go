package node

import (
	"crypto/rsa"
	"time"

	"github.com/linguohua/titan/api"
	titanRsa "github.com/linguohua/titan/node/rsa"
	"github.com/linguohua/titan/node/scheduler/db/persistent"
	"github.com/linguohua/titan/region"

	"github.com/filecoin-project/go-jsonrpc"
)

// var dataDefaultTag = "-1"

// Location Edge node
type Location struct {
	NodeAPI   api.Locator
	Closer    jsonrpc.ClientCloser
	LocatorID string
}

// EdgeNode Edge node
type EdgeNode struct {
	NodeAPI api.Edge
	Closer  jsonrpc.ClientCloser

	Node
}

// CandidateNode Candidate node
type CandidateNode struct {
	NodeAPI api.Candidate
	Closer  jsonrpc.ClientCloser
	// isValidator bool

	Node
}

// Node Common
type Node struct {
	DeviceInfo      api.DevicesInfo
	GeoInfo         *region.GeoInfo
	Addr            string
	LastRequestTime time.Time

	CacheStat                 api.CacheStat
	CacheTimeoutTimeStamp     int64 // TimeStamp of cache timeout
	CacheNextTimeoutTimeStamp int64 // TimeStamp of next cache timeout
	PrivateKey                *rsa.PrivateKey
	DownloadSrvURL            string
}

// node online
func (n *Node) setNodeOnline(typeName api.NodeTypeName) error {
	deviceID := n.DeviceInfo.DeviceId
	geoInfo := n.GeoInfo

	err := persistent.GetDB().SetNodeInfo(deviceID, &persistent.NodeInfo{
		Geo:        geoInfo.Geo,
		LastTime:   time.Now(),
		IsOnline:   true,
		NodeType:   string(typeName),
		Address:    n.Addr,
		PrivateKey: titanRsa.PrivateKey2Pem(n.PrivateKey),
		URL:        n.DownloadSrvURL,
		Exited:     false,
	})
	if err != nil {
		return err
	}

	return nil
}

// node offline
func (n *Node) setNodeOffline(deviceID string, geoInfo *region.GeoInfo, nodeType api.NodeTypeName, lastTime time.Time) {
	// err := cache.GetDB().RemoveNodeWithGeoList(deviceID, geoInfo.Geo)
	// if err != nil {
	// 	log.Warnf("node offline RemoveNodeWithGeoList err : %v ,deviceID : %v", err.Error(), deviceID)
	// }

	err := persistent.GetDB().SetNodeInfo(deviceID, &persistent.NodeInfo{
		Geo:      geoInfo.Geo,
		LastTime: lastTime,
		IsOnline: false,
		NodeType: string(nodeType),
		Address:  n.Addr,
		Exited:   false,
	})
	if err != nil {
		log.Errorf("node offline SetNodeInfo err : %s ,deviceID : %s", err.Error(), deviceID)
	}
}

// getNodeInfo  get node information
func (n *Node) getNodeInfo(deviceID string) (*persistent.NodeInfo, error) {
	node, err := persistent.GetDB().GetNodeInfo(deviceID)
	if err != nil {
		log.Errorf("getNodeInfo: %s ,deviceID : %s", err.Error(), deviceID)
	}
	return node, nil
}

func (n *Node) updateAccessAuth(access *api.DownloadServerAccessAuth) error {
	return persistent.GetDB().SetNodeAuthInfo(access)
}

// UpdateCacheStat Update Cache Stat
func (n *Node) UpdateCacheStat(info api.CacheStat) {
	n.CacheStat = info

	num := info.WaitCacheBlockNum + info.DoingCacheBlockNum

	timeStamp := time.Now().Unix()
	n.CacheTimeoutTimeStamp = timeStamp + int64(num*info.DownloadTimeout*info.RetryNum)

	n.CacheNextTimeoutTimeStamp = n.CacheTimeoutTimeStamp + int64(info.DownloadTimeout*info.RetryNum)
}
