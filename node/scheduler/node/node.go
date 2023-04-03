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

// Node edge and candidate node
type Node struct {
	*API
	jsonrpc.ClientCloser

	token string
	*BaseInfo
}

// API node api
type API struct {
	api.Common
	api.Device
	api.Validate
	api.DataSync
	api.CarfileOperation
	WaitQuiet func(ctx context.Context) error
	// edge
	ExternalServiceAddress func(ctx context.Context, schedulerURL string) (string, error)
	UserNATTravel          func(ctx context.Context, sourceURL string, req *types.NatTravelReq) error
	// candidate
	GetBlocksOfCarfile func(ctx context.Context, carfileCID string, randomSeed int64, randomCount int) (map[int]string, error)
	ValidateNodes      func(ctx context.Context, reqs []api.ValidateReq) (string, error)
}

// New new node
func New(token string) *Node {
	node := &Node{
		token: token,
	}

	return node
}

// APIFromEdge node api from edge api
func APIFromEdge(api api.Edge) *API {
	a := &API{
		Common:                 api,
		Device:                 api,
		Validate:               api,
		DataSync:               api,
		CarfileOperation:       api,
		WaitQuiet:              api.WaitQuiet,
		ExternalServiceAddress: api.ExternalServiceAddress,
		UserNATTravel:          api.UserNATTravel,
	}
	return a
}

// APIFromCandidate node api from candidate api
func APIFromCandidate(api api.Candidate) *API {
	a := &API{
		Common:             api,
		Device:             api,
		Validate:           api,
		DataSync:           api,
		CarfileOperation:   api,
		WaitQuiet:          api.WaitQuiet,
		GetBlocksOfCarfile: api.GetBlocksOfCarfile,
		ValidateNodes:      api.ValidateNodes,
	}
	return a
}

// ConnectRPC connect node rpc
func (n *Node) ConnectRPC(addr string, isNodeConnect bool, nodeType types.NodeType) error {
	if !isNodeConnect {
		if addr == n.remoteAddr {
			return nil
		}

		n.remoteAddr = addr

		// close old
		if n.ClientCloser != nil {
			n.ClientCloser()
		}
	}
	rpcURL := fmt.Sprintf("https://%s/rpc/v0", addr)

	headers := http.Header{}
	headers.Add("Authorization", "Bearer "+n.token)

	if nodeType == types.NodeEdge {
		// Connect to node
		edgeAPI, closer, err := client.NewEdge(context.Background(), rpcURL, headers)
		if err != nil {
			return xerrors.Errorf("NewEdge err:%s,url:%s", err.Error(), rpcURL)
		}

		n.API = APIFromEdge(edgeAPI)
		n.ClientCloser = closer
		return nil
	}

	if nodeType == types.NodeCandidate {
		// Connect to node
		candidateAPI, closer, err := client.NewCandidate(context.Background(), rpcURL, headers)
		if err != nil {
			return xerrors.Errorf("NewCandidate err:%s,url:%s", err.Error(), rpcURL)
		}

		n.API = APIFromCandidate(candidateAPI)
		n.ClientCloser = closer
		return nil
	}

	return xerrors.Errorf("node %s type %d not wrongful", n.NodeInfo.NodeID, n.NodeType)
}

// BaseInfo Common
type BaseInfo struct {
	*types.NodeInfo
	publicKey  *rsa.PublicKey
	remoteAddr string
	TCPAddr    string

	lastRequestTime time.Time
	pullingCount    int // The number of asset waiting and pulling in progress

	selectCode int
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

// RemoteAddr rpc addr
func (n *BaseInfo) RemoteAddr() string {
	return n.remoteAddr
}

// RPCURL rpc url
func (n *BaseInfo) RPCURL() string {
	return fmt.Sprintf("https://%s/rpc/v0", n.remoteAddr)
}

// DownloadAddr download address
func (n *BaseInfo) DownloadAddr() string {
	addr := n.remoteAddr
	if n.PortMapping != "" {
		index := strings.Index(n.remoteAddr, ":")
		ip := n.remoteAddr[:index+1]
		addr = ip + n.PortMapping
	}

	return addr
}

// LastRequestTime get node last request time
func (n *BaseInfo) LastRequestTime() time.Time {
	return n.lastRequestTime
}

// SetLastRequestTime set node last request time
func (n *BaseInfo) SetLastRequestTime(t time.Time) {
	n.lastRequestTime = t
}

// SetCurPullingCount set node pulling count
func (n *BaseInfo) SetCurPullingCount(t int) {
	n.pullingCount = t
}

// IncrCurPullingCount Incr pulling count
func (n *BaseInfo) IncrCurPullingCount(v int) {
	n.pullingCount += v
}

// CurPullingCount pulling count
func (n *BaseInfo) CurPullingCount() int {
	return n.pullingCount
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
