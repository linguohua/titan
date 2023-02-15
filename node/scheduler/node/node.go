package node

import (
	"context"
	"crypto/rsa"
	"fmt"
	"net/http"
	"time"

	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/api/client"
	titanRsa "github.com/linguohua/titan/node/rsa"
	"github.com/linguohua/titan/node/scheduler/db/cache"
	"github.com/linguohua/titan/node/scheduler/db/persistent"
	"github.com/linguohua/titan/region"
	"golang.org/x/xerrors"

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
	nodeAPI    api.Edge
	closer     jsonrpc.ClientCloser
	adminToken []byte
	*Node
}

// NewEdgeNode new edge
func NewEdgeNode(adminToken []byte) *EdgeNode {
	edgeNode := &EdgeNode{
		adminToken: adminToken,
	}

	return edgeNode
}

// ConnectRPC connect node rpc
func (e *EdgeNode) ConnectRPC(addr string) (api.Edge, error) {
	if addr == e.remoteAddr {
		return nil, xerrors.New("the address has not changed")
	}

	e.remoteAddr = addr

	rpcURL := fmt.Sprintf("https://%s/rpc/v0", addr)

	headers := http.Header{}
	headers.Add("Authorization", "Bearer "+string(e.adminToken))

	// Connect to node
	edgeAPI, closer, err := client.NewEdge(context.Background(), rpcURL, headers)
	if err != nil {
		return nil, xerrors.Errorf("NewEdge err:%s,url:%s", err.Error(), rpcURL)
	}

	e.nodeAPI = edgeAPI
	e.closer = closer

	return edgeAPI, nil
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
	nodeAPI    api.Candidate
	closer     jsonrpc.ClientCloser
	adminToken []byte
	*Node
}

// NewCandidateNode new candidate
func NewCandidateNode(adminToken []byte) *CandidateNode {
	candidateNode := &CandidateNode{
		adminToken: adminToken,
	}

	return candidateNode
}

// ConnectRPC connect node rpc
func (c *CandidateNode) ConnectRPC(addr string) (api.Edge, error) {
	if addr == c.remoteAddr {
		return nil, xerrors.New("the address has not changed")
	}

	c.remoteAddr = addr

	rpcURL := fmt.Sprintf("https://%s/rpc/v0", addr)

	headers := http.Header{}
	headers.Add("Authorization", "Bearer "+string(c.adminToken))

	// Connect to node
	edgeAPI, closer, err := client.NewCandicate(context.Background(), rpcURL, headers)
	if err != nil {
		return nil, xerrors.Errorf("NewCandicate err:%s,url:%s", err.Error(), rpcURL)
	}

	c.nodeAPI = edgeAPI
	c.closer = closer

	return edgeAPI, nil
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
	privateKey    *rsa.PrivateKey
	nodeType      api.NodeTypeName
	remoteAddr    string

	geoInfo         *region.GeoInfo
	lastRequestTime time.Time
	cacheStat       *api.CacheStat
	curCacheCount   int // The number of caches waiting and in progress
}

// NewNode new
func NewNode(deviceInfo *api.DevicesInfo, remoteAddr string, privateKey *rsa.PrivateKey, nodeType api.NodeTypeName, geoInfo *region.GeoInfo) *Node {
	node := &Node{
		remoteAddr: remoteAddr,
		privateKey: privateKey,
		nodeType:   nodeType,
		geoInfo:    geoInfo,

		DeviceID:      deviceInfo.DeviceId,
		DiskUsage:     deviceInfo.DiskUsage,
		BandwidthDown: deviceInfo.BandwidthDown,
		BandwidthUp:   deviceInfo.BandwidthUp,
	}

	return node
}

// GetPrivateKey get private key
func (n *Node) GetPrivateKey() *rsa.PrivateKey {
	return n.privateKey
}

// GetRPCURL rpc url
func (n *Node) GetRPCURL() string {
	return fmt.Sprintf("https://%s/rpc/v0", n.remoteAddr)
}

// GetDownloadURL download url
func (n *Node) GetDownloadURL() string {
	return fmt.Sprintf("https://%s/block/get", n.remoteAddr)
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
		Address:    n.remoteAddr,
		PrivateKey: titanRsa.PrivateKey2Pem(n.privateKey),
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
