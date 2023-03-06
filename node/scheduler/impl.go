package scheduler

import (
	"context"
	"crypto/rsa"
	"fmt"
	"net"
	"net/http"

	"github.com/linguohua/titan/node/modules/dtypes"

	"go.uber.org/fx"

	"github.com/gbrlsnchs/jwt/v3"
	"github.com/linguohua/titan/node/config"
	"github.com/linguohua/titan/node/scheduler/election"
	"github.com/linguohua/titan/node/scheduler/locator"
	"github.com/linguohua/titan/node/scheduler/validator"

	// "github.com/linguohua/titan/node/device"

	"github.com/filecoin-project/go-jsonrpc/auth"
	logging "github.com/ipfs/go-log/v2"
	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/api/client"
	"github.com/linguohua/titan/node/common"
	"github.com/linguohua/titan/node/handler"
	titanRsa "github.com/linguohua/titan/node/rsa"
	"github.com/linguohua/titan/node/scheduler/node"

	"github.com/linguohua/titan/node/scheduler/carfile"
	"github.com/linguohua/titan/node/scheduler/db/persistent"
	"github.com/linguohua/titan/node/scheduler/sync"
	"golang.org/x/xerrors"
)

var log = logging.Logger("scheduler")

const (
	// StatusOffline node offline
	StatusOffline = "offline"
	// StatusOnline node online
	StatusOnline = "online"
	// seconds
	blockDonwloadTimeout = 30 * 60
)

type blockDownloadVerifyStatus int

const (
	blockDownloadStatusUnknow blockDownloadVerifyStatus = iota
	blockDownloadStatusFailed
	blockDownloadStatusSucceeded
)

// NewLocalScheduleNode ...
//func NewLocalScheduleNode(lr repo.LockedRepo, SchedulerCfg *config.SchedulerCfg) api.Scheduler {
//	s := &Scheduler{}
//
//	NodeManager := node.NewManager(s.nodeExitedCallback)
//	s.CommonAPI = common.NewCommonAPI(NodeManager.NodeSessionCallBack)
//	s.Web = web.NewWeb(s)
//
//	sec, err := secret.APISecret(lr)
//	if err != nil {
//		log.Panicf("NewLocalScheduleNode failed:%s", err.Error())
//	}
//	s.APISecret = sec
//
//	// area.InitServerArea(areaStr)
//
//	err = s.authNew()
//	if err != nil {
//		log.Panicf("authNew err:%s", err.Error())
//	}
//
//	s.NodeManager = NodeManager
//	s.Election = Election.NewElection(NodeManager)
//	s.Validator = Validator.NewElection(NodeManager, false)
//	s.DataManager = carfile.NewManager(NodeManager, s.WriteToken)
//	s.DataSync = sync.NewDataSync(NodeManager)
//	s.SchedulerCfg = SchedulerCfg
//
//	s.AppUpdater, err = persistent.GetNodeUpdateInfos()
//	if err != nil {
//		log.Errorf("GetNodeUpdateInfos error:%s", err)
//	}
//
//	return s
//}

// Scheduler node
type Scheduler struct {
	fx.In

	api.Web
	*common.CommonAPI
	*AppUpdater

	NodeManager  *node.Manager
	Election     *election.Election
	Validator    *validator.Validator
	DataManager  *carfile.Manager
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
	err := s.NodeManager.CarfileDB.GetNodeAllocateInfo(nodeID, persistent.SecretKey, &secret)
	if err != nil {
		return nil, xerrors.Errorf("JWT Verification %s GetRegisterInfo failed: %w", nodeID, err)
	}

	deviceSecret := secret

	if _, err := jwt.Verify([]byte(token), (*jwt.HMACSHA)(jwt.NewHS256([]byte(deviceSecret))), &payload); err != nil {
		return nil, xerrors.Errorf("JWT Verification failed: %w", err)
	}
	return payload.Allow, nil
}

// AuthNodeNew  Get Node Auth
func (s *Scheduler) AuthNodeNew(ctx context.Context, perms []auth.Permission, nodeID, deviceSecret string) ([]byte, error) {
	p := jwtPayload{
		Allow: perms,
	}

	var secret string
	err := s.NodeManager.CarfileDB.GetNodeAllocateInfo(nodeID, persistent.SecretKey, &secret)
	if err != nil {
		return nil, xerrors.Errorf("JWT Verification %s GetRegisterInfo failed: %w", nodeID, err)
	}

	if secret != deviceSecret {
		return nil, xerrors.Errorf("device %s secret not match", nodeID)
	}

	return jwt.Sign(&p, (*jwt.HMACSHA)(jwt.NewHS256([]byte(deviceSecret))))
}

// CandidateNodeConnect Candidate connect
func (s *Scheduler) CandidateNodeConnect(ctx context.Context) error {
	remoteAddr := handler.GetRemoteAddr(ctx)
	nodeID := handler.GetNodeID(ctx)

	if !s.deviceExists(nodeID, int(api.NodeCandidate)) {
		return xerrors.Errorf("candidate node not Exist: %s", nodeID)
	}

	log.Infof("Candidate Connect %s, address:%s", nodeID, remoteAddr)
	candidateNode := node.NewCandidate(s.AdminToken)
	candicateAPI, err := candidateNode.ConnectRPC(remoteAddr, true)
	if err != nil {
		return xerrors.Errorf("CandidateNodeConnect ConnectRPC err:%s", err.Error())
	}

	// load device info
	deviceInfo, err := candicateAPI.DeviceInfo(ctx)
	if err != nil {
		log.Errorf("CandidateNodeConnect DeviceInfo err:%s", err.Error())
		return err
	}

	if nodeID != deviceInfo.NodeID {
		return xerrors.Errorf("nodeID mismatch %s,%s", nodeID, deviceInfo.NodeID)
	}

	privateKeyStr, err := s.NodeManager.NodeMgrDB.NodePrivateKey(nodeID)
	if err != nil {
		return xerrors.Errorf("NodePrivateKey %s,%s", nodeID, err.Error())
	}
	var privateKey *rsa.PrivateKey
	if len(privateKeyStr) > 0 {
		privateKey, err = titanRsa.Pem2PrivateKey(privateKeyStr)
		if err != nil {
			return err
		}
	} else {
		key, err := titanRsa.GeneratePrivateKey(1024)
		if err != nil {
			return err
		}
		privateKey = key
	}

	port, err := s.NodeManager.NodeMgrDB.NodePortMapping(nodeID)
	if err != nil {
		return err
	}

	deviceInfo.PortMapping = port

	deviceInfo.NodeType = api.NodeCandidate
	deviceInfo.ExternalIP, _, err = net.SplitHostPort(remoteAddr)
	if err != nil {
		return xerrors.Errorf("SplitHostPort err:%s", err.Error())
	}

	candidateNode.BaseInfo = node.NewBaseInfo(&deviceInfo, privateKey, remoteAddr)

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

	if !s.deviceExists(nodeID, int(api.NodeEdge)) {
		return xerrors.Errorf("edge node not Exist: %s", nodeID)
	}

	log.Infof("Edge Connect %s; remoteAddr:%s", nodeID, remoteAddr)
	edgeNode := node.NewEdge(s.AdminToken)
	edgeAPI, err := edgeNode.ConnectRPC(remoteAddr, true)
	if err != nil {
		return xerrors.Errorf("EdgeNodeConnect ConnectRPC err:%s", err.Error())
	}

	// load device info
	deviceInfo, err := edgeAPI.DeviceInfo(ctx)
	if err != nil {
		log.Errorf("EdgeNodeConnect DeviceInfo err:%s", err.Error())
		return err
	}

	if nodeID != deviceInfo.NodeID {
		return xerrors.Errorf("nodeID mismatch %s,%s", nodeID, deviceInfo.NodeID)
	}

	privateKeyStr, err := s.NodeManager.NodeMgrDB.NodePrivateKey(nodeID)
	if err != nil {
		return xerrors.Errorf("NodePrivateKey %s,%s", nodeID, err.Error())
	}
	var privateKey *rsa.PrivateKey
	if len(privateKeyStr) > 0 {
		privateKey, err = titanRsa.Pem2PrivateKey(privateKeyStr)
		if err != nil {
			return err
		}
	} else {
		key, err := titanRsa.GeneratePrivateKey(1024)
		if err != nil {
			return err
		}
		privateKey = key
	}

	port, err := s.NodeManager.NodeMgrDB.NodePortMapping(nodeID)
	if err != nil {
		return err
	}

	deviceInfo.PortMapping = port

	deviceInfo.NodeType = api.NodeEdge
	deviceInfo.ExternalIP, _, err = net.SplitHostPort(remoteAddr)
	if err != nil {
		return xerrors.Errorf("SplitHostPort err:%s", err.Error())
	}

	edgeNode.BaseInfo = node.NewBaseInfo(&deviceInfo, privateKey, remoteAddr)

	err = s.NodeManager.EdgeOnline(edgeNode)
	if err != nil {
		log.Errorf("EdgeNodeConnect addEdgeNode err:%s,nodeID:%s", err.Error(), deviceInfo.NodeID)
		return err
	}

	// notify locator
	locator.ChangeNodeOnlineStatus(nodeID, true)

	s.DataSync.Add2List(nodeID)

	return nil
}

// GetPublicKey get node Public Key
func (s *Scheduler) GetPublicKey(ctx context.Context) (string, error) {
	nodeID := handler.GetNodeID(ctx)

	edgeNode := s.NodeManager.GetEdgeNode(nodeID)
	if edgeNode != nil {
		return titanRsa.PublicKey2Pem(&edgeNode.PrivateKey().PublicKey), nil
	}

	candidateNode := s.NodeManager.GetCandidateNode(nodeID)
	if candidateNode != nil {
		return titanRsa.PublicKey2Pem(&candidateNode.PrivateKey().PublicKey), nil
	}

	return "", fmt.Errorf("Can not get node %s publicKey", nodeID)
}

// GetExternalAddr get node External address
func (s *Scheduler) GetExternalAddr(ctx context.Context) (string, error) {
	remoteAddr := handler.GetRemoteAddr(ctx)
	return remoteAddr, nil
}

// ValidateBlockResult Validator Block Result
func (s *Scheduler) ValidateBlockResult(ctx context.Context, validateResults api.ValidateResults) error {
	validator := handler.GetNodeID(ctx)
	log.Debug("call back Validator block result, Validator is", validator)
	if !s.deviceExists(validator, 0) {
		return xerrors.Errorf("node not Exist: %s", validator)
	}

	vs := &validateResults
	vs.Validator = validator

	// s.Validator.PushResultToQueue(vs)
	s.Validator.ValidateResult(vs)
	return nil
}

func (s *Scheduler) AllocateNodes(ctx context.Context, nodeType api.NodeType, count int) ([]api.NodeAllocateInfo, error) {
	list := make([]api.NodeAllocateInfo, 0)
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

// GetOnlineNodeIDs Get all online node id
func (s *Scheduler) GetOnlineNodeIDs(ctx context.Context, nodeType api.NodeType) ([]string, error) {
	if nodeType == api.NodeValidate {
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

	return s.NodeManager.GetOnlineNodes(nodeType)
}

// ElectionValidators Validators
func (s *Scheduler) ElectionValidators(ctx context.Context) error {
	s.Election.StartElect()
	return nil
}

// GetDevicesInfo return the devices information
func (s *Scheduler) GetDevicesInfo(ctx context.Context, nodeID string) (api.DeviceInfo, error) {
	// node datas
	deviceInfo, err := s.NodeManager.NodeMgrDB.LoadNodeInfo(nodeID)
	if err != nil {
		log.Errorf("getNodeInfo: %s ,nodeID : %s", err.Error(), nodeID)
		return api.DeviceInfo{}, err
	}

	isOnline := s.NodeManager.GetCandidateNode(nodeID) != nil
	if !isOnline {
		isOnline = s.NodeManager.GetEdgeNode(nodeID) != nil
	}

	deviceInfo.DeviceStatus = getDeviceStatus(isOnline)

	return *deviceInfo, nil
}

// GetDeviceStatus return the status of the device
func getDeviceStatus(isOnline bool) string {
	switch isOnline {
	case true:
		return StatusOnline
	default:
		return StatusOffline
	}
}

// ValidateSwitch  open or close Validator task
func (s *Scheduler) ValidateSwitch(ctx context.Context, enable bool) error {
	return nil
}

// ValidateRunningState get Validator running state,
// false is close
// true is open
func (s *Scheduler) ValidateRunningState(ctx context.Context) (bool, error) {
	// the framework requires that the method must return error
	return s.SchedulerCfg.EnableValidate, nil
}

// ValidateStart start once Validator
func (s *Scheduler) ValidateStart(ctx context.Context) error {
	return s.Validator.StartValidateOnceTask()
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

// GetDownloadInfo get node download info
func (s *Scheduler) GetDownloadInfo(ctx context.Context, nodeID string) ([]*api.BlockDownloadInfo, error) {
	return s.NodeManager.CarfileDB.GetBlockDownloadInfoByNodeID(nodeID)
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
		log.Infof("need restore carfile :%s", hash)
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

// ShowNodeLogFile show node log file
func (s *Scheduler) ShowNodeLogFile(ctx context.Context, nodeID string) (*api.LogFile, error) {
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

// DownloadNodeLogFile Download Node Log File
func (s *Scheduler) DownloadNodeLogFile(ctx context.Context, nodeID string) ([]byte, error) {
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

// deviceExists Check if the id exists
func (s *Scheduler) deviceExists(nodeID string, nodeType int) bool {
	var nType int
	err := s.NodeManager.CarfileDB.GetNodeAllocateInfo(nodeID, persistent.NodeTypeKey, &nType)
	if err != nil {
		return false
	}

	if nodeType != 0 {
		return nType == nodeType
	}

	return true
}
