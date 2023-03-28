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

	"go.uber.org/fx"

	"github.com/gbrlsnchs/jwt/v3"
	"github.com/linguohua/titan/node/config"
	"github.com/linguohua/titan/node/scheduler/assets"
	"github.com/linguohua/titan/node/scheduler/election"
	"github.com/linguohua/titan/node/scheduler/locator"
	"github.com/linguohua/titan/node/scheduler/validation"

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

// Scheduler node
type Scheduler struct {
	fx.In

	*common.CommonAPI
	*EdgeUpdater
	dtypes.ServerID

	NodeManager            *node.Manager
	Election               *election.Election
	Validation             *validation.Validation
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

// NodeAuthVerify Verify Node Auth
func (s *Scheduler) NodeAuthVerify(ctx context.Context, token string) ([]auth.Permission, error) {
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

// NodeAuthNew  Get Node Auth
func (s *Scheduler) NodeAuthNew(ctx context.Context, nodeID, sign string) (string, error) {
	p := jwtPayload{
		Allow:  api.ReadWritePerms,
		NodeID: nodeID,
	}

	pem, err := s.NodeManager.LoadNodePublicKey(nodeID)
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

// CandidateNodeConnect Candidate connect
func (s *Scheduler) CandidateNodeConnect(ctx context.Context, token string) error {
	remoteAddr := handler.GetRemoteAddr(ctx)
	nodeID := handler.GetNodeID(ctx)

	baseInfo := s.NodeManager.GetNode(nodeID)
	if baseInfo != nil {
		oAddr := baseInfo.Addr()
		if oAddr != remoteAddr {
			return xerrors.Errorf("node already login, addr : %s", oAddr)
		}
	} else {
		if !s.nodeExists(nodeID, types.NodeCandidate) {
			return xerrors.Errorf("candidate node not exists: %s", nodeID)
		}
	}

	log.Infof("candidate connected %s, address:%s", nodeID, remoteAddr)
	candidateNode := node.NewCandidate(token)
	candidateAPI, err := candidateNode.ConnectRPC(remoteAddr, true)
	if err != nil {
		return xerrors.Errorf("CandidateNodeConnect ConnectRPC err:%s", err.Error())
	}

	// load node info
	nodeInfo, err := candidateAPI.NodeInfo(ctx)
	if err != nil {
		log.Errorf("CandidateNodeConnect NodeInfo err:%s", err.Error())
		return err
	}

	nodeInfo.NodeType = types.NodeCandidate
	nodeInfo.ServerID = s.ServerID

	if baseInfo == nil {
		baseInfo, err = s.getNodeBaseInfo(nodeID, remoteAddr, &nodeInfo)
		if err != nil {
			return err
		}
	}

	candidateNode.BaseInfo = baseInfo

	err = s.NodeManager.CandidateOnline(candidateNode)
	if err != nil {
		log.Errorf("CandidateNodeConnect CandidateOnline err:%s,nodeID:%s", err.Error(), nodeID)
		return err
	}

	// notify locator
	locator.ChangeNodeOnlineStatus(nodeID, true)

	s.DataSync.Add2List(nodeID)

	return nil
}

// EdgeNodeConnect edge connect
func (s *Scheduler) EdgeNodeConnect(ctx context.Context, token string) error {
	remoteAddr := handler.GetRemoteAddr(ctx)
	nodeID := handler.GetNodeID(ctx)

	baseInfo := s.NodeManager.GetNode(nodeID)
	if baseInfo != nil {
		oAddr := baseInfo.Addr()
		if oAddr != remoteAddr {
			return xerrors.Errorf("node already login, addr : %s", oAddr)
		}
	} else {
		if !s.nodeExists(nodeID, types.NodeEdge) {
			return xerrors.Errorf("edge node not exists: %s", nodeID)
		}
	}

	log.Infof("edge connected %s; remoteAddr:%s", nodeID, remoteAddr)
	edgeNode := node.NewEdge(token)
	edgeAPI, err := edgeNode.ConnectRPC(remoteAddr, true)
	if err != nil {
		return xerrors.Errorf("EdgeNodeConnect ConnectRPC err:%s", err.Error())
	}

	// load node info
	nodeInfo, err := edgeAPI.NodeInfo(ctx)
	if err != nil {
		log.Errorf("EdgeNodeConnect NodeInfo err:%s", err.Error())
		return err
	}

	nodeInfo.NodeType = types.NodeEdge
	nodeInfo.ServerID = s.ServerID

	if baseInfo == nil {
		baseInfo, err = s.getNodeBaseInfo(nodeID, remoteAddr, &nodeInfo)
		if err != nil {
			return err
		}
	}

	edgeNode.BaseInfo = baseInfo

	err = s.NodeManager.EdgeOnline(edgeNode)
	if err != nil {
		log.Errorf("EdgeNodeConnect EdgeOnline err:%s,nodeID:%s", err.Error(), nodeInfo.NodeID)
		return err
	}

	// notify locator
	locator.ChangeNodeOnlineStatus(nodeID, true)

	s.DataSync.Add2List(nodeID)

	return nil
}

func (s *Scheduler) getNodeBaseInfo(nodeID, remoteAddr string, nodeInfo *types.NodeInfo) (*node.BaseInfo, error) {
	if nodeID != nodeInfo.NodeID {
		return nil, xerrors.Errorf("nodeID mismatch %s, %s", nodeID, nodeInfo.NodeID)
	}

	port, err := s.NodeManager.LoadPortMapping(nodeID)
	if err != nil && err != sql.ErrNoRows {
		return nil, xerrors.Errorf("load node port %s err : %s", nodeID, err.Error())
	}

	pStr, err := s.NodeManager.LoadNodePublicKey(nodeID)
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

	return node.NewBaseInfo(nodeInfo, publicKey, remoteAddr), nil
}

// NodeExternalServiceAddress get node External address
func (s *Scheduler) NodeExternalServiceAddress(ctx context.Context) (string, error) {
	remoteAddr := handler.GetRemoteAddr(ctx)
	return remoteAddr, nil
}

// NodeValidatedResult Validator Block Result
func (s *Scheduler) NodeValidatedResult(ctx context.Context, result api.ValidateResult) error {
	validator := handler.GetNodeID(ctx)
	log.Debug("call back Validator block result, Validator is ", validator)

	vs := &result
	vs.Validator = validator

	// s.Validator.PushResultToQueue(vs)
	return s.Validation.Result(vs)
}

// RegisterNode Register Node , Returns an error if the node is already registered
func (s *Scheduler) RegisterNode(ctx context.Context, nodeID, pKey string, nodeType types.NodeType) error {
	return s.NodeManager.InsertNodeRegisterInfo(pKey, nodeID, nodeType)
}

// OnlineNodeList Get all online node id
func (s *Scheduler) OnlineNodeList(ctx context.Context, nodeType types.NodeType) ([]string, error) {
	if nodeType == types.NodeValidator {
		return s.NodeManager.LoadValidators(s.ServerID)
	}

	return s.NodeManager.OnlineNodeList(nodeType)
}

// StartOnceElection Validators
func (s *Scheduler) StartOnceElection(ctx context.Context) error {
	s.Election.StartElect()
	return nil
}

// NodeInfo return the node information
func (s *Scheduler) NodeInfo(ctx context.Context, nodeID string) (types.NodeInfo, error) {
	nodeInfo := types.NodeInfo{}
	nodeInfo.Online = false

	info := s.NodeManager.GetNode(nodeID)
	if info != nil {
		nodeInfo = *info.NodeInfo
		nodeInfo.Online = true
	} else {
		dbInfo, err := s.NodeManager.LoadNodeInfo(nodeID)
		if err != nil {
			log.Errorf("getNodeInfo: %s ,nodeID : %s", err.Error(), nodeID)
			return types.NodeInfo{}, err
		}

		nodeInfo = *dbInfo
	}

	return nodeInfo, nil
}

// LocatorConnect Locator Connect
func (s *Scheduler) LocatorConnect(ctx context.Context, id string, token string) error {
	remoteAddr := handler.GetRemoteAddr(ctx)
	url := fmt.Sprintf("https://%s/rpc/v0", remoteAddr)

	log.Infof("LocatorConnect locatorID:%s, addr:%s", id, remoteAddr)

	headers := http.Header{}
	headers.Add("Authorization", "Bearer "+token)
	// Connect to scheduler
	// log.Infof("EdgeNodeConnect edge url:%v", url)
	locatorAPI, closer, err := client.NewLocator(ctx, url, headers)
	if err != nil {
		log.Errorf("LocatorConnect err:%s,url:%s", err.Error(), url)
		return err
	}

	locator.StoreLocator(locator.New(locatorAPI, closer, id))

	return nil
}

// NodeQuit node want to quit titan
func (s *Scheduler) NodeQuit(ctx context.Context, nodeID string) error {
	s.NodeManager.NodesQuit([]string{nodeID})

	return nil
}

// SetNodePort set node port
func (s *Scheduler) SetNodePort(ctx context.Context, nodeID, port string) error {
	baseInfo := s.NodeManager.GetNode(nodeID)
	if baseInfo != nil {
		baseInfo.SetNodePort(port)
	}

	return s.NodeManager.SetPortMapping(nodeID, port)
}

// nodeExists Check if the id exists
func (s *Scheduler) nodeExists(nodeID string, nodeType types.NodeType) bool {
	err := s.NodeManager.NodeExists(nodeID, nodeType)
	if err != nil {
		log.Errorf("node exists %s", err.Error())
		return false
	}

	return true
}

// NodeList list nodes
func (s *Scheduler) NodeList(ctx context.Context, offset int, limit int) (*types.ListNodesRsp, error) {
	rsp := &types.ListNodesRsp{Data: make([]types.NodeInfo, 0)}

	rows, total, err := s.NodeManager.LoadNodeInfos(limit, offset)
	if err != nil {
		return rsp, err
	}

	validator := make(map[string]struct{})
	validatorList, err := s.NodeManager.LoadValidators(s.NodeManager.ServerID)
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

// ValidatedResultList get validated result infos
func (s *Scheduler) ValidatedResultList(ctx context.Context, startTime, endTime time.Time, pageNumber, pageSize int) (*types.ListValidateResultRsp, error) {
	svm, err := s.NodeManager.LoadValidateResultInfos(startTime, endTime, pageNumber, pageSize)
	if err != nil {
		return nil, err
	}

	return svm, nil
}

// PublicKey get server publicKey
func (s *Scheduler) PublicKey(ctx context.Context) (string, error) {
	if s.PrivateKey == nil {
		return "", fmt.Errorf("scheduler private key not exist")
	}

	publicKey := s.PrivateKey.PublicKey
	pem := titanrsa.PublicKey2Pem(&publicKey)
	return string(pem), nil
}

func (s *Scheduler) SubmitProofOfWork(ctx context.Context, proofs []*types.NodeWorkloadProof) error {
	return nil
}

// FindCandidateDownloadSources find candidate sources
func (s *Scheduler) FindCandidateDownloadSources(ctx context.Context, cid string) ([]*types.DownloadSource, error) {
	hash, err := cidutil.CIDString2HashString(cid)
	if err != nil {
		return nil, xerrors.Errorf("%s cid to hash err:%s", cid, err.Error())
	}

	titanRsa := titanrsa.New(crypto.SHA256, crypto.SHA256.New())
	sources := make([]*types.DownloadSource, 0)

	rows, err := s.NodeManager.LoadReplicasOfHash(hash, []types.ReplicaStatus{types.ReplicaStatusSucceeded})
	if err != nil {
		return nil, err
	}

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
