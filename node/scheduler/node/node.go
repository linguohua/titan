package node

import (
	"crypto/rsa"
	"time"

	"github.com/linguohua/titan/api"
	titanRsa "github.com/linguohua/titan/node/rsa"
	"github.com/linguohua/titan/node/scheduler/db/cache"
	"github.com/linguohua/titan/node/scheduler/db/persistent"
	"github.com/linguohua/titan/region"

	"github.com/filecoin-project/go-jsonrpc"
)

// var dataDefaultTag = "-1"

// Locator Edge node
type Locator struct {
	nodeAPI api.Locator
	closer  jsonrpc.ClientCloser

	locatorID string
}

// NewLocation new location
func NewLocation(api api.Locator, closer jsonrpc.ClientCloser, locatorID string) *Locator {
	location := &Locator{
		nodeAPI:   api,
		closer:    closer,
		locatorID: locatorID,
	}

	return location
}

// GetAPI get node api
func (l *Locator) GetAPI() api.Locator {
	return l.nodeAPI
}

// GetLocatorID get id
func (l *Locator) GetLocatorID() string {
	return l.locatorID
}

// EdgeNode Edge node
type EdgeNode struct {
	nodeAPI api.Edge
	closer  jsonrpc.ClientCloser

	*Node
}

// NewEdgeNode new edge
func NewEdgeNode(candicateAPI api.Edge, closer jsonrpc.ClientCloser, node *Node) *EdgeNode {
	edgeNode := &EdgeNode{
		nodeAPI: candicateAPI,
		closer:  closer,

		Node: node,
	}

	return edgeNode
}

// GetAPI get node api
func (e *EdgeNode) GetAPI() api.Edge {
	return e.nodeAPI
}

// ClientCloser get node client closer
func (e *EdgeNode) ClientCloser() {
	e.closer()
}

// CandidateNode Candidate node
type CandidateNode struct {
	nodeAPI api.Candidate
	closer  jsonrpc.ClientCloser
	// isValidator bool

	*Node
}

// NewCandidateNode new candidate
func NewCandidateNode(candicateAPI api.Candidate, closer jsonrpc.ClientCloser, node *Node) *CandidateNode {
	candidateNode := &CandidateNode{
		nodeAPI: candicateAPI,
		closer:  closer,

		Node: node,
	}

	return candidateNode
}

// GetAPI get node api
func (c *CandidateNode) GetAPI() api.Candidate {
	return c.nodeAPI
}

// ClientCloser get node client closer
func (c *CandidateNode) ClientCloser() {
	c.closer()
}

// Node Common
type Node struct {
	DeviceID      string
	DiskUsage     float64
	BandwidthDown float64
	BandwidthUp   float64
	// *api.DevicesInfo
	addr           string
	privateKey     *rsa.PrivateKey
	nodeType       api.NodeTypeName
	downloadSrvURL string

	geoInfo         *region.GeoInfo
	lastRequestTime time.Time

	cacheStat *api.CacheStat
	// cacheTimeoutTimeStamp     int64 // TimeStamp of cache timeout
	// cacheNextTimeoutTimeStamp int64 // TimeStamp of next cache timeout

	curCacheCount int //The number of caches waiting and in progress
}

// NewNode new
func NewNode(deviceInfo *api.DevicesInfo, rpcURL, downloadSrvURL string, privateKey *rsa.PrivateKey, nodeType api.NodeTypeName, geoInfo *region.GeoInfo) *Node {
	node := &Node{
		addr: rpcURL,
		// DevicesInfo:    deviceInfo,
		downloadSrvURL: downloadSrvURL,
		privateKey:     privateKey,
		nodeType:       nodeType,
		geoInfo:        geoInfo,

		DeviceID:      deviceInfo.DeviceId,
		DiskUsage:     deviceInfo.DiskUsage,
		BandwidthDown: deviceInfo.BandwidthDown,
		BandwidthUp:   deviceInfo.BandwidthUp,
	}

	return node
}

// // GetDeviceInfo get device info
// func (n *Node) GetDeviceInfo() *api.DevicesInfo {
// 	return n.deviceInfo
// }

// GetPrivateKey get private key
func (n *Node) GetPrivateKey() *rsa.PrivateKey {
	return n.privateKey
}

// GetAddress get address
func (n *Node) GetAddress() string {
	return n.addr
}

// // GetCacheTimeoutTimeStamp get cache timeout stamp
// func (n *Node) GetCacheTimeoutTimeStamp() int64 {
// 	return n.cacheTimeoutTimeStamp
// }

// // GetCacheNextTimeoutTimeStamp get cache timeout stamp with next cache
// func (n *Node) GetCacheNextTimeoutTimeStamp() int64 {
// 	return n.cacheNextTimeoutTimeStamp
// }

// GetLastRequestTime get node last request time
func (n *Node) GetLastRequestTime() time.Time {
	return n.lastRequestTime
}

// SetLastRequestTime set node last request time
func (n *Node) SetLastRequestTime(t time.Time) {
	n.lastRequestTime = t
}

// GetGeoInfo get geo info
func (n *Node) GetGeoInfo() *region.GeoInfo {
	return n.geoInfo
}

// SetGeoInfo set geo info
func (n *Node) SetGeoInfo(info *region.GeoInfo) {
	n.geoInfo = info
}

// SetCurCacheCount set cache count
func (n *Node) SetCurCacheCount(t int) {
	n.curCacheCount = t
}

// IncrCurCacheCount Incr cache count
func (n *Node) IncrCurCacheCount(v int) {
	n.curCacheCount += v
}

// GetCurCacheCount cache count
func (n *Node) GetCurCacheCount() int {
	return n.curCacheCount
}

// node online
func (n *Node) setNodeOnline() error {
	deviceID := n.DeviceID
	geoInfo := n.geoInfo
	typeName := string(n.nodeType)

	err := persistent.GetDB().SetNodeInfo(deviceID, &persistent.NodeInfo{
		Geo:        geoInfo.Geo,
		LastTime:   time.Now(),
		IsOnline:   true,
		NodeType:   typeName,
		Address:    n.addr,
		PrivateKey: titanRsa.PrivateKey2Pem(n.privateKey),
		URL:        n.downloadSrvURL,
		Quitted:    false,
	})
	if err != nil {
		return err
	}

	return nil
}

// node offline
func (n *Node) setNodeOffline() {
	deviceID := n.DeviceID

	err := persistent.GetDB().SetNodeOffline(deviceID, n.lastRequestTime)
	if err != nil {
		log.Errorf("node offline SetNodeOffline err : %s ,deviceID : %s", err.Error(), deviceID)
	}
}

// SaveInfo Save Device Info
func (n *Node) SaveInfo(info *api.DevicesInfo) error {
	err := cache.GetDB().SetDeviceInfo(info)
	if err != nil {
		log.Errorf("set device info: %s", err.Error())
		return err
	}

	return nil
}
