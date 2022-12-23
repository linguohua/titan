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
	nodeAPI api.Edge
	closer  jsonrpc.ClientCloser

	Node
}

// NewEdgeNode new edge
func NewEdgeNode(candicateAPI api.Edge, closer jsonrpc.ClientCloser, node Node) *EdgeNode {
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

	Node
}

// NewCandidateNode new candidate
func NewCandidateNode(candicateAPI api.Candidate, closer jsonrpc.ClientCloser, node Node) *CandidateNode {
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
	deviceInfo     api.DevicesInfo
	addr           string
	privateKey     *rsa.PrivateKey
	nodeType       api.NodeTypeName
	downloadSrvURL string

	geoInfo         *region.GeoInfo
	lastRequestTime time.Time

	cacheStat                 api.CacheStat
	cacheTimeoutTimeStamp     int64 // TimeStamp of cache timeout
	cacheNextTimeoutTimeStamp int64 // TimeStamp of next cache timeout
}

// NewNode new
func NewNode(deviceInfo api.DevicesInfo, rpcURL, downloadSrvURL string, privateKey *rsa.PrivateKey, nodeType api.NodeTypeName) Node {
	node := Node{
		addr:           rpcURL,
		deviceInfo:     deviceInfo,
		downloadSrvURL: downloadSrvURL,
		privateKey:     privateKey,
		nodeType:       nodeType,
	}

	return node
}

// GetDeviceInfo get device info
func (n *Node) GetDeviceInfo() api.DevicesInfo {
	return n.deviceInfo
}

// GetPrivateKey get private key
func (n *Node) GetPrivateKey() *rsa.PrivateKey {
	return n.privateKey
}

// GetAddress get address
func (n *Node) GetAddress() string {
	return n.addr
}

// GetCacheTimeoutTimeStamp get cache timeout stamp
func (n *Node) GetCacheTimeoutTimeStamp() int64 {
	return n.cacheTimeoutTimeStamp
}

// GetCacheNextTimeoutTimeStamp get cache timeout stamp with next cache
func (n *Node) GetCacheNextTimeoutTimeStamp() int64 {
	return n.cacheNextTimeoutTimeStamp
}

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

// node online
func (n *Node) setNodeOnline() error {
	deviceID := n.deviceInfo.DeviceId
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
		Exited:     false,
	})
	if err != nil {
		return err
	}

	return nil
}

// node offline
func (n *Node) setNodeOffline() {
	deviceID := n.deviceInfo.DeviceId

	err := persistent.GetDB().SetNodeOffline(deviceID, n.lastRequestTime)
	if err != nil {
		log.Errorf("node offline SetNodeOffline err : %s ,deviceID : %s", err.Error(), deviceID)
	}
}

// UpdateCacheStat Update Cache Stat
func (n *Node) UpdateCacheStat(info api.CacheStat) {
	n.cacheStat = info

	num := info.WaitCacheBlockNum + info.DoingCacheBlockNum

	timeStamp := time.Now().Unix()
	n.cacheTimeoutTimeStamp = timeStamp + int64(num*info.DownloadTimeout*info.RetryNum)

	n.cacheNextTimeoutTimeStamp = n.cacheTimeoutTimeStamp + int64(info.DownloadTimeout*info.RetryNum)
}
