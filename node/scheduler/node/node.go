package node

import (
	"bytes"
	"context"
	"crypto/rsa"
	"encoding/gob"
	"encoding/hex"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/api/client"
	"github.com/linguohua/titan/api/types"
	titanrsa "github.com/linguohua/titan/node/rsa"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/go-jsonrpc"
)

// Edge Edge node
type Edge struct {
	nodeAPI api.Edge
	closer  jsonrpc.ClientCloser
	token   string
	*BaseInfo
}

// NewEdge new edge
func NewEdge(token string) *Edge {
	edgeNode := &Edge{
		token: token,
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
	headers.Add("Authorization", "Bearer "+e.token)

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
	nodeAPI api.Candidate
	closer  jsonrpc.ClientCloser
	token   string
	*BaseInfo
}

// NewCandidate new candidate
func NewCandidate(token string) *Candidate {
	candidateNode := &Candidate{
		token: token,
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
	headers.Add("Authorization", "Bearer "+c.token)

	// Connect to node
	candidateAPI, closer, err := client.NewCandidate(context.Background(), rpcURL, headers)
	if err != nil {
		return nil, xerrors.Errorf("NewCandidate err:%s,url:%s", err.Error(), rpcURL)
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
	*types.NodeInfo
	publicKey  *rsa.PublicKey
	remoteAddr string

	lastRequestTime time.Time
	cacheCount      int // The number of caches waiting and in progress
}

// NewBaseInfo new
func NewBaseInfo(nodeInfo *types.NodeInfo, pKey *rsa.PublicKey, addr string) *BaseInfo {
	bi := &BaseInfo{
		NodeInfo:   nodeInfo,
		publicKey:  pKey,
		remoteAddr: addr,
	}

	return bi
}

// PublicKey get publicKey key
func (n *BaseInfo) PublicKey() *rsa.PublicKey {
	return n.publicKey
}

// Addr rpc url
func (n *BaseInfo) Addr() string {
	return n.remoteAddr
}

// RPCURL rpc url
func (n *BaseInfo) RPCURL() string {
	return fmt.Sprintf("https://%s/rpc/v0", n.remoteAddr)
}

// DownloadURL download url
func (n *BaseInfo) DownloadURL() string {
	addr := n.remoteAddr
	if n.PortMapping != "" {
		index := strings.Index(n.remoteAddr, ":")
		ip := n.remoteAddr[:index+1]
		addr = ip + n.PortMapping
	}

	return fmt.Sprintf("https://%s", addr)
}

// LastRequestTime get node last request time
func (n *BaseInfo) LastRequestTime() time.Time {
	return n.lastRequestTime
}

// SetLastRequestTime set node last request time
func (n *BaseInfo) SetLastRequestTime(t time.Time) {
	n.lastRequestTime = t
}

// SetCurCacheCount set NodeMgrCache count
func (n *BaseInfo) SetCurCacheCount(t int) {
	n.cacheCount = t
}

// IncrCurCacheCount Incr NodeMgrCache count
func (n *BaseInfo) IncrCurCacheCount(v int) {
	n.cacheCount += v
}

// CurCacheCount NodeMgrCache count
func (n *BaseInfo) CurCacheCount() int {
	return n.cacheCount
}

// SetNodePort reset node port
func (n *BaseInfo) SetNodePort(port string) {
	n.PortMapping = port
}

// Credentials get Credentials
func (n *BaseInfo) Credentials(cid string, titanRsa *titanrsa.Rsa, privateKey *rsa.PrivateKey) (*types.GatewayCredentials, error) {
	svc := &types.Credentials{
		ID:        uuid.NewString(),
		NodeID:    n.NodeID,
		CarCID:    cid,
		ValidTime: time.Now().Add(10 * time.Hour).Unix(),
	}

	b, err := n.encryptCredentials(svc, n.publicKey, titanRsa)
	if err != nil {
		return nil, xerrors.Errorf("%s encryptCredentials err:%s", n.NodeID, err.Error())
	}

	sign, err := titanRsa.Sign(privateKey, b)
	if err != nil {
		return nil, xerrors.Errorf("%s Sign err:%s", n.NodeID, err.Error())
	}

	return &types.GatewayCredentials{Ciphertext: hex.EncodeToString(b), Sign: hex.EncodeToString(sign)}, nil
}

func (n *BaseInfo) encryptCredentials(at *types.Credentials, publicKey *rsa.PublicKey, rsa *titanrsa.Rsa) ([]byte, error) {
	var buffer bytes.Buffer
	enc := gob.NewEncoder(&buffer)
	err := enc.Encode(at)
	if err != nil {
		return nil, err
	}

	return rsa.Encrypt(buffer.Bytes(), publicKey)
}
