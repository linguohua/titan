package scheduler

import (
	"context"
	"crypto"
	"crypto/rsa"
	"database/sql"
	"encoding/hex"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/linguohua/titan/node/cidutil"
	"github.com/linguohua/titan/node/modules/dtypes"
	"github.com/linguohua/titan/node/scheduler/validate"

	"go.uber.org/fx"

	"github.com/gbrlsnchs/jwt/v3"
	"github.com/linguohua/titan/node/config"
	"github.com/linguohua/titan/node/scheduler/assets"

	"github.com/filecoin-project/go-jsonrpc/auth"
	logging "github.com/ipfs/go-log/v2"
	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/api/client"
	"github.com/linguohua/titan/api/types"
	"github.com/linguohua/titan/node/common"
	"github.com/linguohua/titan/node/handler"
	"github.com/linguohua/titan/node/scheduler/node"

	titanrsa "github.com/linguohua/titan/node/rsa"
	"github.com/linguohua/titan/node/scheduler/sync"
	"golang.org/x/xerrors"
)

var log = logging.Logger("scheduler")

// Scheduler represents a scheduler node in a distributed system.
type Scheduler struct {
	fx.In

	*common.CommonAPI
	*EdgeUpdateManager
	dtypes.ServerID

	NodeManager            *node.Manager
	ValidateMgr            *validate.Manager
	AssetManager           *assets.Manager
	DataSync               *sync.DataSync
	SchedulerCfg           *config.SchedulerCfg
	SetSchedulerConfigFunc dtypes.SetSchedulerConfigFunc
	GetSchedulerConfigFunc dtypes.GetSchedulerConfigFunc

	PrivateKey *rsa.PrivateKey
}

var _ api.Scheduler = &Scheduler{}

type jwtPayload struct {
	Allow  []auth.Permission
	NodeID string
}

// VerifyNodeAuthToken verifies the JWT token for a node.
func (s *Scheduler) VerifyNodeAuthToken(ctx context.Context, token string) ([]auth.Permission, error) {
	nodeID := handler.GetNodeID(ctx)

	var payload jwtPayload
	if _, err := jwt.Verify([]byte(token), s.APISecret, &payload); err != nil {
		return nil, xerrors.Errorf("node:%s JWT Verify failed: %w", nodeID, err)
	}

	if payload.NodeID != nodeID {
		return nil, xerrors.Errorf("node id %s not match", nodeID)
	}

	return payload.Allow, nil
}

// CreateNodeAuthToken creates a new JWT token for a node.
func (s *Scheduler) CreateNodeAuthToken(ctx context.Context, nodeID, sign string) (string, error) {
	p := jwtPayload{
		Allow:  api.ReadWritePerms,
		NodeID: nodeID,
	}

	pem, err := s.NodeManager.FetchNodePublicKey(nodeID)
	if err != nil {
		return "", xerrors.Errorf("%s load node public key failed: %w", nodeID, err)
	}

	publicKey, err := titanrsa.Pem2PublicKey([]byte(pem))
	if err != nil {
		return "", err
	}

	signBuf, err := hex.DecodeString(sign)
	if err != nil {
		return "", err
	}

	rsa := titanrsa.New(crypto.SHA256, crypto.SHA256.New())
	err = rsa.VerifySign(publicKey, signBuf, []byte(nodeID))
	if err != nil {
		return "", err
	}

	tk, err := jwt.Sign(&p, s.APISecret)
	if err != nil {
		return "", xerrors.Errorf("node %s sign err:%s", nodeID, err.Error())
	}

	return string(tk), nil
}

// processes a node connection request with the given options and node type.
func (s *Scheduler) nodeConnect(ctx context.Context, opts *types.ConnectOptions, nodeType types.NodeType) error {
	remoteAddr := handler.GetRemoteAddr(ctx)
	nodeID := handler.GetNodeID(ctx)

	oldNode := s.NodeManager.GetNode(nodeID)
	if oldNode != nil {
		oAddr := oldNode.RemoteAddr()
		if oAddr != remoteAddr {
			return xerrors.Errorf("node already login, addr : %s", oAddr)
		}
	} else {
		if !s.nodeExists(nodeID, nodeType) {
			return xerrors.Errorf("node not exists: %s , type: %d", nodeID, nodeType)
		}
	}
	cNode := node.New(opts.Token)

	log.Infof("node connected %s, address:%s", nodeID, remoteAddr)

	err := cNode.ConnectRPC(remoteAddr, true, nodeType)
	if err != nil {
		return xerrors.Errorf("nodeConnect ConnectRPC err:%s", err.Error())
	}

	if oldNode == nil {
		// load node info
		nodeInfo, err := cNode.API.NodeInfo(ctx)
		if err != nil {
			log.Errorf("nodeConnect NodeInfo err:%s", err.Error())
			return err
		}

		nodeInfo.NodeType = nodeType
		nodeInfo.ServerID = s.ServerID

		baseInfo, err := s.getNodeBaseInfo(nodeID, remoteAddr, &nodeInfo, opts.TcpServerPort)
		if err != nil {
			return err
		}

		if nodeType == types.NodeEdge {
			natType := s.determineNATType(context.Background(), cNode.API, remoteAddr)
			baseInfo.NatType = natType.String()
		}

		cNode.BaseInfo = baseInfo

		err = s.NodeManager.NodeOnline(cNode)
		if err != nil {
			log.Errorf("nodeConnect err:%s,nodeID:%s", err.Error(), nodeID)
			return err
		}
	}

	s.DataSync.AddNodeToList(nodeID)

	return nil
}

// ConnectCandidateNode processes a candidate node connection request.
func (s *Scheduler) ConnectCandidateNode(ctx context.Context, opts *types.ConnectOptions) error {
	return s.nodeConnect(ctx, opts, types.NodeCandidate)
}

// ConnectEdgeNode  processes a edge node connection request.
func (s *Scheduler) ConnectEdgeNode(ctx context.Context, opts *types.ConnectOptions) error {
	return s.nodeConnect(ctx, opts, types.NodeEdge)
}

func (s *Scheduler) getNodeBaseInfo(nodeID, remoteAddr string, nodeInfo *types.NodeInfo, tcpPort int) (*node.BaseInfo, error) {
	if nodeID != nodeInfo.NodeID {
		return nil, xerrors.Errorf("nodeID mismatch %s, %s", nodeID, nodeInfo.NodeID)
	}

	port, err := s.NodeManager.FetchPortMapping(nodeID)
	if err != nil && err != sql.ErrNoRows {
		return nil, xerrors.Errorf("load node port %s err : %s", nodeID, err.Error())
	}

	pStr, err := s.NodeManager.FetchNodePublicKey(nodeID)
	if err != nil && err != sql.ErrNoRows {
		return nil, xerrors.Errorf("load node port %s err : %s", nodeID, err.Error())
	}

	publicKey, err := titanrsa.Pem2PublicKey([]byte(pStr))
	if err != nil {
		return nil, xerrors.Errorf("load node port %s err : %s", nodeID, err.Error())
	}

	nodeInfo.PortMapping = port

	nodeInfo.ExternalIP, _, err = net.SplitHostPort(remoteAddr)
	if err != nil {
		return nil, xerrors.Errorf("SplitHostPort err:%s", err.Error())
	}

	return node.NewBaseInfo(nodeInfo, publicKey, remoteAddr, tcpPort), nil
}

// GetNodeExternalAddress retrieves the external address of the node.
func (s *Scheduler) GetNodeExternalAddress(ctx context.Context) (string, error) {
	remoteAddr := handler.GetRemoteAddr(ctx)
	return remoteAddr, nil
}

// ProcessNodeValidationResult  processes the validation result from the node.
func (s *Scheduler) ProcessNodeValidationResult(ctx context.Context, result api.ValidateResult) error {
	validator := handler.GetNodeID(ctx)
	log.Debug("call back Validator block result, Validator is ", validator)

	vs := &result
	vs.Validator = validator

	// s.Validator.PushResultToQueue(vs)
	return s.ValidateMgr.Result(vs)
}

// RegisterNewNode registers a new node, returning an error if the node is already registered.
func (s *Scheduler) RegisterNewNode(ctx context.Context, nodeID, pKey string, nodeType types.NodeType) error {
	return s.NodeManager.InsertNodeRegisterInfo(pKey, nodeID, nodeType)
}

// GetOnlineNodeCount retrieves online node count.
func (s *Scheduler) GetOnlineNodeCount(ctx context.Context, nodeType types.NodeType) (int, error) {
	if nodeType == types.NodeValidator {
		list, err := s.NodeManager.FetchValidators(s.ServerID)
		if err != nil {
			return 0, err
		}

		i := 0
		for _, nodeID := range list {
			node := s.NodeManager.GetCandidateNode(nodeID)
			if node != nil {
				i++
			}
		}

		return i, nil
	}

	return s.NodeManager.GetOnlineNodeCount(nodeType), nil
}

// TriggerElection triggers a single election for validators.
func (s *Scheduler) TriggerElection(ctx context.Context) error {
	s.ValidateMgr.StartElection()
	return nil
}

// RetrieveNodeInfo returns information about the specified node.
func (s *Scheduler) RetrieveNodeInfo(ctx context.Context, nodeID string) (types.NodeInfo, error) {
	nodeInfo := types.NodeInfo{}
	nodeInfo.Online = false

	info := s.NodeManager.GetNode(nodeID)
	if info != nil {
		nodeInfo = *info.NodeInfo
		nodeInfo.Online = true
	} else {
		dbInfo, err := s.NodeManager.FetchNodeInfo(nodeID)
		if err != nil {
			log.Errorf("getNodeInfo: %s ,nodeID : %s", err.Error(), nodeID)
			return types.NodeInfo{}, err
		}

		nodeInfo = *dbInfo
	}

	return nodeInfo, nil
}

// ConnectLocator processes a locator connection request.
func (s *Scheduler) ConnectLocator(ctx context.Context, id string, token string) error {
	remoteAddr := handler.GetRemoteAddr(ctx)
	url := fmt.Sprintf("https://%s/rpc/v0", remoteAddr)

	log.Infof("ConnectLocator locatorID:%s, addr:%s", id, remoteAddr)

	headers := http.Header{}
	headers.Add("Authorization", "Bearer "+token)
	// Connect to scheduler
	// log.Infof("ConnectEdgeNode edge url:%v", url)
	_, _, err := client.NewLocator(ctx, url, headers)
	if err != nil {
		log.Errorf("ConnectLocator err:%s,url:%s", err.Error(), url)
		return err
	}

	return nil
}

// NodeQuit node want to quit titan
func (s *Scheduler) NodeQuit(ctx context.Context, nodeID string) error {
	s.NodeManager.NodesQuit([]string{nodeID})

	return nil
}

// UpdateNodePort sets the port for the specified node.
func (s *Scheduler) UpdateNodePort(ctx context.Context, nodeID, port string) error {
	baseInfo := s.NodeManager.GetNode(nodeID)
	if baseInfo != nil {
		baseInfo.UpdateNodePort(port)
	}

	return s.NodeManager.SetPortMapping(nodeID, port)
}

// nodeExists checks if the node with the specified ID exists.
func (s *Scheduler) nodeExists(nodeID string, nodeType types.NodeType) bool {
	err := s.NodeManager.NodeExists(nodeID, nodeType)
	if err != nil {
		log.Errorf("node exists %s", err.Error())
		return false
	}

	return true
}

// GetNodeList retrieves a list of nodes with pagination.
func (s *Scheduler) GetNodeList(ctx context.Context, offset int, limit int) (*types.ListNodesRsp, error) {
	rsp := &types.ListNodesRsp{Data: make([]types.NodeInfo, 0)}

	rows, total, err := s.NodeManager.FetchNodeInfos(limit, offset)
	if err != nil {
		return rsp, err
	}
	defer rows.Close()

	validator := make(map[string]struct{})
	validatorList, err := s.NodeManager.FetchValidators(s.NodeManager.ServerID)
	if err != nil {
		log.Errorf("get validator list: %v", err)
	}
	for _, id := range validatorList {
		validator[id] = struct{}{}
	}

	nodeInfos := make([]types.NodeInfo, 0)
	for rows.Next() {
		nodeInfo := &types.NodeInfo{}
		err = rows.StructScan(nodeInfo)
		if err != nil {
			log.Errorf("NodeInfo StructScan err: %s", err.Error())
			continue
		}

		_, exist := validator[nodeInfo.NodeID]
		if exist {
			nodeInfo.NodeType = types.NodeValidator
		}

		nodeInfos = append(nodeInfos, *nodeInfo)
	}

	rsp.Data = nodeInfos
	rsp.Total = total

	return rsp, nil
}

// GetValidationResultList retrieves a list of validation results.
func (s *Scheduler) GetValidationResultList(ctx context.Context, startTime, endTime time.Time, pageNumber, pageSize int) (*types.ListValidateResultRsp, error) {
	svm, err := s.NodeManager.FetchValidateResultInfos(startTime, endTime, pageNumber, pageSize)
	if err != nil {
		return nil, err
	}

	return svm, nil
}

// GetServerPublicKey get server publicKey
func (s *Scheduler) GetServerPublicKey(ctx context.Context) (string, error) {
	if s.PrivateKey == nil {
		return "", fmt.Errorf("scheduler private key not exist")
	}

	publicKey := s.PrivateKey.PublicKey
	pem := titanrsa.PublicKey2Pem(&publicKey)
	return string(pem), nil
}

// IgnoreProofOfWork does nothing and is a placeholder for future implementation.
func (s *Scheduler) IgnoreProofOfWork(ctx context.Context, proofs []*types.NodeWorkloadProof) error {
	return nil
}

// RetrieveCandidateDownloadSources finds candidate download sources for the given CID.
func (s *Scheduler) RetrieveCandidateDownloadSources(ctx context.Context, cid string) ([]*types.DownloadSource, error) {
	hash, err := cidutil.CIDString2HashString(cid)
	if err != nil {
		return nil, xerrors.Errorf("%s cid to hash err:%s", cid, err.Error())
	}

	titanRsa := titanrsa.New(crypto.SHA256, crypto.SHA256.New())
	sources := make([]*types.DownloadSource, 0)

	rows, err := s.NodeManager.FetchReplicasByHash(hash, []types.ReplicaStatus{types.ReplicaStatusSucceeded})
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		rInfo := &types.ReplicaInfo{}
		err = rows.StructScan(rInfo)
		if err != nil {
			log.Errorf("replica StructScan err: %s", err.Error())
			continue
		}

		if !rInfo.IsCandidate {
			continue
		}

		nodeID := rInfo.NodeID
		cNode := s.NodeManager.GetCandidateNode(nodeID)
		if cNode == nil {
			continue
		}

		credentials, err := cNode.Credentials(cid, titanRsa, s.NodeManager.PrivateKey)
		if err != nil {
			continue
		}
		source := &types.DownloadSource{
			CandidateAddr: cNode.DownloadAddr(),
			Credentials:   credentials,
		}

		sources = append(sources, source)
	}

	return sources, nil
}

// GetAssetListForBucket retrieves a list of assets for the specified node's bucket.
func (s *Scheduler) GetAssetListForBucket(ctx context.Context, nodeID string) ([]string, error) {
	return nil, nil
}
