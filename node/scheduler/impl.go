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

	"github.com/linguohua/titan/node/modules/dtypes"

	"go.uber.org/fx"

	"github.com/gbrlsnchs/jwt/v3"
	"github.com/linguohua/titan/node/config"
	"github.com/linguohua/titan/node/scheduler/election"
	"github.com/linguohua/titan/node/scheduler/locator"
	"github.com/linguohua/titan/node/scheduler/validation"

	// "github.com/linguohua/titan/node/device"

	"github.com/filecoin-project/go-jsonrpc/auth"
	logging "github.com/ipfs/go-log/v2"
	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/api/client"
	"github.com/linguohua/titan/api/types"
	"github.com/linguohua/titan/node/common"
	"github.com/linguohua/titan/node/handler"
	"github.com/linguohua/titan/node/scheduler/node"

	titanrsa "github.com/linguohua/titan/node/rsa"
	"github.com/linguohua/titan/node/scheduler/storage"
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

	NodeManager *node.Manager
	Election    *election.Election
	Validation  *validation.Validation
	DataManager *storage.Manager
	DataSync    *sync.DataSync
	// WriteToken             dtypes.PermissionWriteToken
	AdminToken             dtypes.PermissionAdminToken
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

	pem, err := s.NodeManager.NodeMgrDB.NodePublicKey(nodeID)
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
func (s *Scheduler) CandidateNodeConnect(ctx context.Context) error {
	remoteAddr := handler.GetRemoteAddr(ctx)
	nodeID := handler.GetNodeID(ctx)

	if !s.nodeExists(nodeID, types.NodeCandidate) {
		return xerrors.Errorf("candidate node not exists: %s", nodeID)
	}

	baseInfo := s.NodeManager.GetNode(nodeID)
	if baseInfo != nil {
		oAddr := baseInfo.Addr()
		if oAddr != remoteAddr {
			return xerrors.Errorf("node already login, addr : %s", oAddr)
		}
	}

	log.Infof("candidate connected %s, address:%s", nodeID, remoteAddr)
	candidateNode := node.NewCandidate(string(s.AdminToken))
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
func (s *Scheduler) EdgeNodeConnect(ctx context.Context) error {
	remoteAddr := handler.GetRemoteAddr(ctx)
	nodeID := handler.GetNodeID(ctx)

	if !s.nodeExists(nodeID, types.NodeEdge) {
		return xerrors.Errorf("edge node not exists: %s", nodeID)
	}

	baseInfo := s.NodeManager.GetNode(nodeID)
	if baseInfo != nil {
		oAddr := baseInfo.Addr()
		if oAddr != remoteAddr {
			return xerrors.Errorf("node already login, addr : %s", oAddr)
		}
	}

	log.Infof("edge connected %s; remoteAddr:%s", nodeID, remoteAddr)
	edgeNode := node.NewEdge(string(s.AdminToken))
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

	port, err := s.NodeManager.NodeMgrDB.NodePortMapping(nodeID)
	if err != nil && err != sql.ErrNoRows {
		return nil, xerrors.Errorf("load node port %s err : %s", nodeID, err.Error())
	}

	pStr, err := s.NodeManager.NodeMgrDB.NodePublicKey(nodeID)
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
func (s *Scheduler) NodeValidatedResult(ctx context.Context, result api.ValidatedResult) error {
	validator := handler.GetNodeID(ctx)
	log.Debug("call back Validator block result, Validator is ", validator)
	// if !s.nodeExists(validator, 0) {
	// 	return xerrors.Errorf("node not Exist: %s", validator)
	// }

	vs := &result
	vs.Validator = validator

	// s.Validator.PushResultToQueue(vs)
	s.Validation.Result(vs)
	return nil
}

// RegisterNode Register Node , Returns an error if the node is already registered
func (s *Scheduler) RegisterNode(ctx context.Context, nodeID, pKey string, nodeType types.NodeType) error {
	return s.NodeManager.NodeMgrDB.InsertNode(pKey, nodeID, nodeType)
}

// func (s *Scheduler) AllocateNodes(ctx context.Context, nodeType types.NodeType, count int) ([]*types.NodeAllocateInfo, error) {
// 	list := make([]*types.NodeAllocateInfo, 0)
// 	if count <= 0 || count > 10 {
// 		return list, nil
// 	}

// 	for i := 0; i < count; i++ {
// 		info, err := s.NodeManager.Allocate(nodeType)
// 		if err != nil {
// 			log.Errorf("RegisterNode err:%s", err.Error())
// 			continue
// 		}

// 		list = append(list, info)
// 	}

// 	return list, nil
// }

// OnlineNodeList Get all online node id
func (s *Scheduler) OnlineNodeList(ctx context.Context, nodeType types.NodeType) ([]string, error) {
	if nodeType == types.NodeValidator {
		list, err := s.NodeManager.NodeMgrDB.GetValidatorsWithList(s.ServerID)
		if err != nil {
			return nil, err
		}

		out := make([]string, 0)
		for _, nodeID := range list {
			node := s.NodeManager.GetCandidateNode(nodeID)
			if node != nil {
				out = append(out, nodeID)
			}
		}
		return out, nil
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
		dbInfo, err := s.NodeManager.NodeMgrDB.NodeInfo(nodeID)
		if err != nil {
			log.Errorf("getNodeInfo: %s ,nodeID : %s", err.Error(), nodeID)
			return types.NodeInfo{}, err
		}

		nodeInfo = *dbInfo
	}

	return nodeInfo, nil
}

// SetEnableValidation set validation state
func (s *Scheduler) SetEnableValidation(ctx context.Context, enable bool) error {
	// the framework requires that the method must return error
	cfg, err := s.GetSchedulerConfigFunc()
	if err != nil {
		return err
	}
	cfg.EnableValidate = enable

	return s.SetSchedulerConfigFunc(cfg)
}

// LocatorConnect Locator Connect
func (s *Scheduler) LocatorConnect(ctx context.Context, id string, token string) error {
	remoteAddr := handler.GetRemoteAddr(ctx)
	url := fmt.Sprintf("https://%s/rpc/v0", remoteAddr)

	log.Infof("LocatorConnect locatorID:%s, addr:%s", id, remoteAddr)

	headers := http.Header{}
	headers.Add("Authorization", "Bearer "+string(token))
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

func (s *Scheduler) nodeExitedCallback(nodeIDs []string) {
	// clean node cache
	log.Infof("node event , nodes quit:%v", nodeIDs)

	hashes, err := s.NodeManager.CarfileDB.LoadCarfileRecordsWithNodes(nodeIDs)
	if err != nil {
		log.Errorf("LoadCarfileRecordsWithNodes err:%s", err.Error())
		return
	}

	err = s.NodeManager.CarfileDB.RemoveReplicaInfoWithNodes(nodeIDs)
	if err != nil {
		log.Errorf("RemoveReplicaInfoWithNodes err:%s", err.Error())
		return
	}

	for _, hash := range hashes {
		log.Infof("need restore storage :%s", hash)
	}
}

// SetNodePort set node port
func (s *Scheduler) SetNodePort(ctx context.Context, nodeID, port string) error {
	baseInfo := s.NodeManager.GetNode(nodeID)
	if baseInfo != nil {
		baseInfo.SetNodePort(port)
	}

	return s.NodeManager.NodeMgrDB.SetNodePortMapping(nodeID, port)
}

func (s *Scheduler) authNew() error {
	// wtk, err := s.AuthNew(context.Background(), []auth.Permission{api.PermRead, api.PermWrite})
	// if err != nil {
	// 	log.Errorf("AuthNew err:%s", err.Error())
	// 	return err
	// }

	// s.WriteToken = dtypes.PermissionWriteToken(wtk)

	atk, err := s.AuthNew(context.Background(), api.AllPermissions)
	if err != nil {
		log.Errorf("AuthNew err:%s", err.Error())
		return err
	}

	s.AdminToken = dtypes.PermissionAdminToken(atk)

	return nil
}

// NodeLogFileInfo show node log file
func (s *Scheduler) NodeLogFileInfo(ctx context.Context, nodeID string) (*api.LogFile, error) {
	cNode := s.NodeManager.GetCandidateNode(nodeID)
	if cNode != nil {
		return cNode.API().ShowLogFile(ctx)
	}

	eNode := s.NodeManager.GetEdgeNode(nodeID)
	if eNode != nil {
		return eNode.API().ShowLogFile(ctx)
	}

	return nil, xerrors.Errorf("node %s not found", nodeID)
}

// NodeLogFile Download Node Log File
func (s *Scheduler) NodeLogFile(ctx context.Context, nodeID string) ([]byte, error) {
	cNode := s.NodeManager.GetCandidateNode(nodeID)
	if cNode != nil {
		return cNode.API().DownloadLogFile(ctx)
	}

	eNode := s.NodeManager.GetEdgeNode(nodeID)
	if eNode != nil {
		return eNode.API().DownloadLogFile(ctx)
	}

	return nil, xerrors.Errorf("node %s not found", nodeID)
}

func (s *Scheduler) DeleteNodeLogFile(ctx context.Context, nodeID string) error {
	cNode := s.NodeManager.GetCandidateNode(nodeID)
	if cNode != nil {
		return cNode.API().DeleteLogFile(ctx)
	}

	eNode := s.NodeManager.GetEdgeNode(nodeID)
	if eNode != nil {
		return eNode.API().DeleteLogFile(ctx)
	}

	return xerrors.Errorf("node %s not found", nodeID)
}

// nodeExists Check if the id exists
func (s *Scheduler) nodeExists(nodeID string, nodeType types.NodeType) bool {
	err := s.NodeManager.NodeMgrDB.NodeExists(nodeID, nodeType)
	if err != nil {
		log.Errorf("node exists %s", err.Error())
		return false
	}

	return true
}

// NodeList list nodes
func (s *Scheduler) NodeList(ctx context.Context, cursor int, count int) (*types.ListNodesRsp, error) {
	rsp := &types.ListNodesRsp{Data: make([]types.NodeInfo, 0)}

	nodes, total, err := s.NodeManager.NodeMgrDB.ListNodeIDs(cursor, count)
	if err != nil {
		return rsp, err
	}

	validator := make(map[string]struct{})
	validatorList, err := s.NodeManager.NodeMgrDB.GetValidatorsWithList(s.NodeManager.ServerID)
	if err != nil {
		log.Errorf("get validator list: %v", err)
	}
	for _, id := range validatorList {
		validator[id] = struct{}{}
	}

	nodeInfos := make([]types.NodeInfo, 0)
	for _, nodeID := range nodes {
		nodeInfo, err := s.NodeInfo(ctx, nodeID)
		if err != nil {
			log.Errorf("NodeInfo: %s ,nodeID : %s", err.Error(), nodeID)
			continue
		}

		_, exist := validator[nodeID]
		if exist {
			nodeInfo.NodeType = types.NodeValidator
		}

		nodeInfos = append(nodeInfos, nodeInfo)
	}

	rsp.Data = nodeInfos
	rsp.Total = total

	return rsp, nil
}

// DownloadRecordList lost download record
func (s *Scheduler) DownloadRecordList(ctx context.Context, req types.ListBlockDownloadInfoReq) (*types.ListDownloadRecordRsp, error) {
	startTime := time.Unix(req.StartTime, 0)
	endTime := time.Unix(req.EndTime, 0)

	downloadInfos, total, err := s.NodeManager.CarfileDB.GetBlockDownloadInfos(req.NodeID, startTime, endTime, req.Cursor, req.Count)
	if err != nil {
		return nil, err
	}
	return &types.ListDownloadRecordRsp{
		Data:  downloadInfos,
		Total: total,
	}, nil
}

// CarfileReplicaList list carfile replicas
func (s *Scheduler) CarfileReplicaList(ctx context.Context, req types.ListCacheInfosReq) (*types.ListCarfileReplicaRsp, error) {
	startTime := time.Unix(req.StartTime, 0)
	endTime := time.Unix(req.EndTime, 0)

	info, err := s.NodeManager.CarfileDB.CarfileReplicaList(startTime, endTime, req.Cursor, req.Count)
	if err != nil {
		return nil, err
	}

	return info, nil
}

func (s *Scheduler) SystemInfo(ctx context.Context) (types.SystemBaseInfo, error) {
	// TODO get info from db

	return types.SystemBaseInfo{}, nil
}

func (s *Scheduler) ValidatedResultList(ctx context.Context, startTime, endTime time.Time, pageNumber, pageSize int) (*types.ListValidatedResultRsp, error) {
	svm, err := s.NodeManager.NodeMgrDB.ValidatedResultInfos(startTime, endTime, pageNumber, pageSize)
	if err != nil {
		return nil, err
	}

	return svm, nil
}

func (s *Scheduler) PublicKey(ctx context.Context) (string, error) {
	if s.PrivateKey == nil {
		return "", fmt.Errorf("scheduler private key not exist")
	}

	publicKey := s.PrivateKey.PublicKey
	pem := titanrsa.PublicKey2Pem(&publicKey)
	return string(pem), nil
}
