package node

import (
	"context"
	"crypto/rsa"
	"fmt"
	"net/http"
	"strings"
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

// Edge Edge node
type Edge struct {
	nodeAPI    api.Edge
	closer     jsonrpc.ClientCloser
	adminToken []byte
	*BaseInfo
}

// NewEdge new edge
func NewEdge(adminToken []byte) *Edge {
	edgeNode := &Edge{
		adminToken: adminToken,
	}

	return edgeNode
}

// ConnectRPC connect node rpc
func (e *Edge) ConnectRPC(addr string, isNodeConnect bool) (api.Edge, error) {
	if !isNodeConnect {
		if addr == e.remoteAddr {
			return nil, xerrors.New("the address has not changed")
		}

		e.remoteAddr = addr

		// close old
		if e.closer != nil {
			e.closer()
		}
	}
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

// API get node api
func (e *Edge) API() api.Edge {
	return e.nodeAPI
}

// ClientCloser get node client closer
func (e *Edge) ClientCloser() {
	e.closer()
}

// Candidate Candidate node
type Candidate struct {
	nodeAPI    api.Candidate
	closer     jsonrpc.ClientCloser
	adminToken []byte
	*BaseInfo
}

// NewCandidate new candidate
func NewCandidate(adminToken []byte) *Candidate {
	candidateNode := &Candidate{
		adminToken: adminToken,
	}

	return candidateNode
}

// ConnectRPC connect node rpc
func (c *Candidate) ConnectRPC(addr string, isNodeConnect bool) (api.Candidate, error) {
	if !isNodeConnect {
		if addr == c.remoteAddr {
			return nil, xerrors.New("the address has not changed")
		}

		c.remoteAddr = addr

		// close old
		if c.closer != nil {
			c.closer()
		}
	}

	rpcURL := fmt.Sprintf("https://%s/rpc/v0", addr)

	headers := http.Header{}
	headers.Add("Authorization", "Bearer "+string(c.adminToken))

	// Connect to node
	candidateAPI, closer, err := client.NewCandicate(context.Background(), rpcURL, headers)
	if err != nil {
		return nil, xerrors.Errorf("NewCandicate err:%s,url:%s", err.Error(), rpcURL)
	}

	c.nodeAPI = candidateAPI
	c.closer = closer

	return candidateAPI, nil
}

// API get node api
func (c *Candidate) API() api.Candidate {
	return c.nodeAPI
}

// ClientCloser get node client closer
func (c *Candidate) ClientCloser() {
	c.closer()
}

// BaseInfo Common
type BaseInfo struct {
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
	cacheCount      int // The number of caches waiting and in progress
	port            string
}

// NewBaseInfo new
func NewBaseInfo(deviceInfo *api.DevicesInfo, remoteAddr string, privateKey *rsa.PrivateKey, nodeType api.NodeTypeName, geoInfo *region.GeoInfo) *BaseInfo {
	bi := &BaseInfo{
		remoteAddr: remoteAddr,
		privateKey: privateKey,
		nodeType:   nodeType,
		geoInfo:    geoInfo,

		DeviceID:      deviceInfo.DeviceId,
		DiskUsage:     deviceInfo.DiskUsage,
		BandwidthDown: deviceInfo.BandwidthDown,
		BandwidthUp:   deviceInfo.BandwidthUp,
	}

	return bi
}

// PrivateKey get private key
func (n *BaseInfo) PrivateKey() *rsa.PrivateKey {
	return n.privateKey
}

// Addr rpc url
func (n *BaseInfo) Addr() string {
	if n.port != "" {
		index := strings.Index(n.remoteAddr, ":")
		ip := n.remoteAddr[:index+1]

		return ip + n.port
	}

	return n.remoteAddr
}

// RPCURL rpc url
func (n *BaseInfo) RPCURL() string {
	return fmt.Sprintf("https://%s/rpc/v0", n.Addr())
}

// DownloadURL download url
func (n *BaseInfo) DownloadURL() string {
	return fmt.Sprintf("https://%s/block/get", n.Addr())
}

// LastRequestTime get node last request time
func (n *BaseInfo) LastRequestTime() time.Time {
	return n.lastRequestTime
}

// SetLastRequestTime set node last request time
func (n *BaseInfo) SetLastRequestTime(t time.Time) {
	n.lastRequestTime = t
}

// GeoInfo get geo info
func (n *BaseInfo) GeoInfo() *region.GeoInfo {
	return n.geoInfo
}

// SetGeoInfo set geo info
func (n *BaseInfo) SetGeoInfo(info *region.GeoInfo) {
	n.geoInfo = info
}

// SetCurCacheCount set cache count
func (n *BaseInfo) SetCurCacheCount(t int) {
	n.cacheCount = t
}

// IncrCurCacheCount Incr cache count
func (n *BaseInfo) IncrCurCacheCount(v int) {
	n.cacheCount += v
}

// CurCacheCount cache count
func (n *BaseInfo) CurCacheCount() int {
	return n.cacheCount
}

// SetNodePort reset node port
func (n *BaseInfo) SetNodePort(port string) {
	n.port = port
}

// node online
func (n *BaseInfo) setNodeOnline() error {
	deviceID := n.DeviceID
	geoInfo := n.geoInfo
	typeName := string(n.nodeType)

	err := persistent.SetNodeInfo(deviceID, &persistent.NodeInfo{
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

// SaveInfo Save Device Info
func (n *BaseInfo) SaveInfo(info *api.DevicesInfo) error {
	err := cache.SetDeviceInfo(info)
	if err != nil {
		log.Errorf("set device info: %s", err.Error())
		return err
	}

	return nil
}
