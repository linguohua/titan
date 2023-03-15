package scheduler

import (
	"context"
	"crypto/rsa"
	"database/sql"
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
	titanRsa "github.com/linguohua/titan/node/rsa"
	"github.com/linguohua/titan/node/scheduler/node"

	"github.com/linguohua/titan/node/scheduler/db/persistent"
	"github.com/linguohua/titan/node/scheduler/storage"
	"github.com/linguohua/titan/node/scheduler/sync"
	"golang.org/x/xerrors"
)

var log = logging.Logger("scheduler")

const (
	// seconds
	blockDonwloadTimeout = 30 * 60
)

type blockDownloadVerifyStatus int

const (
	blockDownloadStatusUnknow blockDownloadVerifyStatus = iota
	blockDownloadStatusFailed
	blockDownloadStatusSucceeded
)

// Scheduler node
type Scheduler struct {
	fx.In

	*common.CommonAPI
	*EdgeUpdater

	NodeManager  *node.Manager
	Election     *election.Election
	Validation   *validation.Validation
	DataManager  *storage.Manager
	DataSync     *sync.DataSync
	WriteToken   dtypes.PermissionWriteToken
	AdminToken   dtypes.PermissionAdminToken
	SchedulerCfg *config.SchedulerCfg

	dtypes.ServerID
}

var _ api.Scheduler = &Scheduler{}

type jwtPayload struct {
	Allow []auth.Permission
}

// AuthNodeVerify Verify Node Auth
func (s *Scheduler) AuthNodeVerify(ctx context.Context, token string) ([]auth.Permission, error) {
	var payload jwtPayload

	nodeID := handler.GetNodeID(ctx)
	if nodeID == "" {
		if _, err := jwt.Verify([]byte(token), (*jwt.HMACSHA)(s.APISecret), &payload); err != nil {
			return nil, xerrors.Errorf("JWT Verification failed: %w", err)
		}

		return payload.Allow, nil
	}

	var secret string
	err := s.NodeManager.NodeMgrDB.GetNodeAllocateInfo(nodeID, persistent.SecretKey, &secret)
	if err != nil {
		return nil, xerrors.Errorf("JWT Verification %s GetRegisterInfo failed: %w", nodeID, err)
	}

	if _, err := jwt.Verify([]byte(token), (*jwt.HMACSHA)(jwt.NewHS256([]byte(secret))), &payload); err != nil {
		return nil, xerrors.Errorf("JWT Verification failed: %w", err)
	}
	return payload.Allow, nil
}

// AuthNodeNew  Get Node Auth
func (s *Scheduler) AuthNodeNew(ctx context.Context, perms []auth.Permission, nodeID, nodeSecret string) ([]byte, error) {
	p := jwtPayload{
		Allow: perms,
	}

	var secret string
	err := s.NodeManager.NodeMgrDB.GetNodeAllocateInfo(nodeID, persistent.SecretKey, &secret)
	if err != nil {
		return nil, xerrors.Errorf("JWT Verification %s GetRegisterInfo failed: %w", nodeID, err)
	}

	if secret != nodeSecret {
		return nil, xerrors.Errorf("node %s secret not match", nodeID)
	}

	return jwt.Sign(&p, (*jwt.HMACSHA)(jwt.NewHS256([]byte(nodeSecret))))
}

// CandidateNodeConnect Candidate connect
func (s *Scheduler) CandidateNodeConnect(ctx context.Context) error {
	remoteAddr := handler.GetRemoteAddr(ctx)
	nodeID := handler.GetNodeID(ctx)

	if !s.nodeExists(nodeID, int(types.NodeCandidate)) {
		return xerrors.Errorf("candidate node not Exist: %s", nodeID)
	}

	oldInfo := s.NodeManager.GetNode(nodeID)
	if oldInfo != nil {
		oAddr := oldInfo.Addr()
		if oAddr != remoteAddr {
			return xerrors.Errorf("node already login, addr : %s", oAddr)
		}
	}

	log.Infof("Candidate Connect %s, address:%s", nodeID, remoteAddr)
	candidateNode := node.NewCandidate(s.AdminToken)
	candicateAPI, err := candidateNode.ConnectRPC(remoteAddr, true)
	if err != nil {
		return xerrors.Errorf("CandidateNodeConnect ConnectRPC err:%s", err.Error())
	}

	// load node info
	nodeInfo, err := candicateAPI.NodeInfo(ctx)
	if err != nil {
		log.Errorf("CandidateNodeConnect NodeInfo err:%s", err.Error())
		return err
	}

	candidateNode.BaseInfo, err = s.getNodeBaseInfo(nodeID, remoteAddr, &nodeInfo)
	if err != nil {
		return err
	}

	err = s.NodeManager.CandidateOnline(candidateNode)
	if err != nil {
		log.Errorf("CandidateNodeConnect addEdgeNode err:%s,nodeID:%s", err.Error(), nodeID)
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

	if !s.nodeExists(nodeID, int(types.NodeEdge)) {
		return xerrors.Errorf("edge node not Exist: %s", nodeID)
	}

	oldInfo := s.NodeManager.GetNode(nodeID)
	if oldInfo != nil {
		oAddr := oldInfo.Addr()
		if oAddr != remoteAddr {
			return xerrors.Errorf("node already login, addr : %s", oAddr)
		}
	}

	log.Infof("Edge Connect %s; remoteAddr:%s", nodeID, remoteAddr)
	edgeNode := node.NewEdge(s.AdminToken)
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

	edgeNode.BaseInfo, err = s.getNodeBaseInfo(nodeID, remoteAddr, &nodeInfo)
	if err != nil {
		return err
	}

	err = s.NodeManager.EdgeOnline(edgeNode)
	if err != nil {
		log.Errorf("EdgeNodeConnect addEdgeNode err:%s,nodeID:%s", err.Error(), nodeInfo.NodeID)
		return err
	}

	// notify locator
	locator.ChangeNodeOnlineStatus(nodeID, true)

	s.DataSync.Add2List(nodeID)

	return nil
}

func (s *Scheduler) getNodeBaseInfo(nodeID, remoteAddr string, nodeInfo *types.NodeInfo) (*node.BaseInfo, error) {
	if nodeID != nodeInfo.NodeID {
		return nil, xerrors.Errorf("nodeID mismatch %s,%s", nodeID, nodeInfo.NodeID)
	}

	privateKey, err := s.loadOrNewPrivateKey(nodeID)
	if err != nil {
		return nil, xerrors.Errorf("loadOrNewPrivateKey %s err : %s", nodeID, err.Error())
	}

	port, err := s.NodeManager.NodeMgrDB.NodePortMapping(nodeID)
	if err != nil && err != sql.ErrNoRows {
		return nil, xerrors.Errorf("load node port %s err : %s", nodeID, err.Error())
	}

	nodeInfo.PortMapping = port

	nodeInfo.NodeType = types.NodeCandidate
	nodeInfo.ExternalIP, _, err = net.SplitHostPort(remoteAddr)
	if err != nil {
		return nil, xerrors.Errorf("SplitHostPort err:%s", err.Error())
	}

	return node.NewBaseInfo(nodeInfo, privateKey, remoteAddr), nil
}

func (s *Scheduler) loadOrNewPrivateKey(nodeID string) (*rsa.PrivateKey, error) {
	privateKeyStr, err := s.NodeManager.NodeMgrDB.NodePrivateKey(nodeID)
	if err != nil && err != sql.ErrNoRows {
		return nil, err
	}

	var privateKey *rsa.PrivateKey
	if len(privateKeyStr) > 0 {
		privateKey, err = titanRsa.Pem2PrivateKey([]byte(privateKeyStr))
		if err != nil {
			return nil, err
		}
	} else {
		key, err := titanRsa.GeneratePrivateKey(1024)
		if err != nil {
			return nil, err
		}
		privateKey = key
	}

	return privateKey, nil
}

// NodePublicKey get node Public Key
func (s *Scheduler) NodePublicKey(ctx context.Context) (string, error) {
	nodeID := handler.GetNodeID(ctx)

	edgeNode := s.NodeManager.GetEdgeNode(nodeID)
	if edgeNode != nil {
		return string(titanRsa.PublicKey2Pem(&edgeNode.PrivateKey().PublicKey)), nil
	}

	candidateNode := s.NodeManager.GetCandidateNode(nodeID)
	if candidateNode != nil {
		return string(titanRsa.PublicKey2Pem(&candidateNode.PrivateKey().PublicKey)), nil
	}

	return "", fmt.Errorf("Can not get node %s publicKey", nodeID)
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

func (s *Scheduler) AllocateNodes(ctx context.Context, nodeType types.NodeType, count int) ([]*types.NodeAllocateInfo, error) {
	list := make([]*types.NodeAllocateInfo, 0)
	if count <= 0 || count > 10 {
		return list, nil
	}

	for i := 0; i < count; i++ {
		info, err := s.NodeManager.Allocate(nodeType)
		if err != nil {
			log.Errorf("RegisterNode err:%s", err.Error())
			continue
		}

		list = append(list, info)
	}

	return list, nil
}

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
func (s *Scheduler) NodeInfo(ctx context.Context, nodeID string) (*types.NodeInfo, error) {
	nodeInfo := &types.NodeInfo{}
	nodeInfo.Online = false

	info := s.NodeManager.GetNode(nodeID)
	if info != nil {
		nodeInfo = info.NodeInfo
		nodeInfo.Online = true
	} else {
		// node datas
		dbInfo, err := s.NodeManager.NodeMgrDB.NodeInfo(nodeID)
		if err != nil {
			log.Errorf("getNodeInfo: %s ,nodeID : %s", err.Error(), nodeID)
			return nil, err
		}

		nodeInfo = dbInfo
	}

	return nodeInfo, nil
}

// ValidationEnable get Validator running state,
// false is close
// true is open
func (s *Scheduler) ValidationEnable(ctx context.Context) (bool, error) {
	// the framework requires that the method must return error
	return s.SchedulerCfg.EnableValidate, nil
}

// StartOnceValidate start once Validator
func (s *Scheduler) StartOnceValidate(ctx context.Context) error {
	return s.Validation.StartValidateOnceTask()
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
func (s *Scheduler) NodeQuit(ctx context.Context, nodeID, secret string) error {
	// TODO Check secret
	s.NodeManager.NodesQuit([]string{nodeID})

	return nil
}

func (s *Scheduler) nodeExitedCallback(nodeIDs []string) {
	// clean node cache
	log.Infof("node event , nodes quit:%v", nodeIDs)

	hashs, err := s.NodeManager.CarfileDB.LoadCarfileRecordsWithNodes(nodeIDs)
	if err != nil {
		log.Errorf("LoadCarfileRecordsWithNodes err:%s", err.Error())
		return
	}

	err = s.NodeManager.CarfileDB.RemoveReplicaInfoWithNodes(nodeIDs)
	if err != nil {
		log.Errorf("RemoveReplicaInfoWithNodes err:%s", err.Error())
		return
	}

	// recache
	for _, hash := range hashs {
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
	wtk, err := s.AuthNew(context.Background(), []auth.Permission{api.PermRead, api.PermWrite})
	if err != nil {
		log.Errorf("AuthNew err:%s", err.Error())
		return err
	}

	s.WriteToken = wtk

	atk, err := s.AuthNew(context.Background(), api.AllPermissions)
	if err != nil {
		log.Errorf("AuthNew err:%s", err.Error())
		return err
	}

	s.AdminToken = atk

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

	return nil, xerrors.Errorf("node %s not found")
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

	return nil, xerrors.Errorf("node %s not found")
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

	return xerrors.Errorf("node %s not found")
}

// nodeExists Check if the id exists
func (s *Scheduler) nodeExists(nodeID string, nodeType int) bool {
	var nType int
	err := s.NodeManager.NodeMgrDB.GetNodeAllocateInfo(nodeID, persistent.NodeTypeKey, &nType)
	if err != nil {
		return false
	}

	if nodeType != 0 {
		return nType == nodeType
	}

	return true
}

// NodeList list nodes
func (s *Scheduler) NodeList(ctx context.Context, cursor int, count int) (*types.ListNodesRsp, error) {
	rsp := &types.ListNodesRsp{Data: make([]*types.NodeInfo, 0)}

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

	nodeInfos := make([]*types.NodeInfo, 0)
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

func (s *Scheduler) ExchangePublicKey(ctx context.Context, nodePublicKey []byte) ([]byte, error) {
	// TODO: generate public key
	return nil, fmt.Errorf("not implement")
}
