package scheduler

import (
	"context"
	"crypto/rsa"
	"fmt"
	"github.com/linguohua/titan/node/modules/dtypes"
	"net"
	"net/http"

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
}

var _ api.Scheduler = &Scheduler{}

type jwtPayload struct {
	Allow []auth.Permission
}

// AuthNodeVerify Verify Node Auth
func (s *Scheduler) AuthNodeVerify(ctx context.Context, token string) ([]auth.Permission, error) {
	var payload jwtPayload

	deviceID := handler.GetDeviceID(ctx)
	if deviceID == "" {
		if _, err := jwt.Verify([]byte(token), (*jwt.HMACSHA)(s.APISecret), &payload); err != nil {
			return nil, xerrors.Errorf("JWT Verification failed: %w", err)
		}

		return payload.Allow, nil
	}

	var secret string
	err := s.NodeManager.CarfileDB.GetNodeAllocateInfo(deviceID, persistent.SecretKey, &secret)
	if err != nil {
		return nil, xerrors.Errorf("JWT Verification %s GetRegisterInfo failed: %w", deviceID, err)
	}

	deviceSecret := secret

	if _, err := jwt.Verify([]byte(token), (*jwt.HMACSHA)(jwt.NewHS256([]byte(deviceSecret))), &payload); err != nil {
		return nil, xerrors.Errorf("JWT Verification failed: %w", err)
	}
	return payload.Allow, nil
}

// AuthNodeNew  Get Node Auth
func (s *Scheduler) AuthNodeNew(ctx context.Context, perms []auth.Permission, deviceID, deviceSecret string) ([]byte, error) {
	p := jwtPayload{
		Allow: perms,
	}

	var secret string
	err := s.NodeManager.CarfileDB.GetNodeAllocateInfo(deviceID, persistent.SecretKey, &secret)
	if err != nil {
		return nil, xerrors.Errorf("JWT Verification %s GetRegisterInfo failed: %w", deviceID, err)
	}

	if secret != deviceSecret {
		return nil, xerrors.Errorf("device %s secret not match", deviceID)
	}

	return jwt.Sign(&p, (*jwt.HMACSHA)(jwt.NewHS256([]byte(deviceSecret))))
}

// CandidateNodeConnect Candidate connect
func (s *Scheduler) CandidateNodeConnect(ctx context.Context) error {
	remoteAddr := handler.GetRemoteAddr(ctx)
	deviceID := handler.GetDeviceID(ctx)

	if !s.deviceExists(deviceID, int(api.NodeCandidate)) {
		return xerrors.Errorf("candidate node not Exist: %s", deviceID)
	}

	log.Infof("Candidate Connect %s, address:%s", deviceID, remoteAddr)
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

	if deviceID != deviceInfo.DeviceID {
		return xerrors.Errorf("deviceID mismatch %s,%s", deviceID, deviceInfo.DeviceID)
	}

	privateKeyStr, err := s.NodeManager.NodeMgrDB.NodePrivateKey(deviceID)
	if err != nil {
		return xerrors.Errorf("NodePrivateKey %s,%s", deviceID, err.Error())
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

	deviceInfo.NodeType = api.NodeCandidate
	deviceInfo.ExternalIP, _, err = net.SplitHostPort(remoteAddr)
	if err != nil {
		return xerrors.Errorf("SplitHostPort err:%s", err.Error())
	}

	// geoInfo, _ := region.GetRegion().GetGeoInfo(deviceInfo.InternalIP)
	// // TODO Does the error need to be handled?

	// deviceInfo.IPLocation = geoInfo.Geo
	// deviceInfo.Longitude = geoInfo.Longitude
	// deviceInfo.Latitude = geoInfo.Latitude

	candidateNode.BaseInfo = node.NewBaseInfo(&deviceInfo, privateKey, remoteAddr)

	err = s.NodeManager.CandidateOnline(candidateNode)
	if err != nil {
		log.Errorf("CandidateNodeConnect addEdgeNode err:%s,deviceID:%s", err.Error(), deviceID)
		return err
	}

	// notify locator
	locator.ChangeNodeOnlineStatus(deviceID, true)

	s.DataSync.Add2List(deviceID)

	return nil
}

// EdgeNodeConnect edge connect
func (s *Scheduler) EdgeNodeConnect(ctx context.Context) error {
	remoteAddr := handler.GetRemoteAddr(ctx)
	deviceID := handler.GetDeviceID(ctx)

	if !s.deviceExists(deviceID, int(api.NodeEdge)) {
		return xerrors.Errorf("edge node not Exist: %s", deviceID)
	}

	log.Infof("Edge Connect %s; remoteAddr:%s", deviceID, remoteAddr)
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

	if deviceID != deviceInfo.DeviceID {
		return xerrors.Errorf("deviceID mismatch %s,%s", deviceID, deviceInfo.DeviceID)
	}

	privateKeyStr, err := s.NodeManager.NodeMgrDB.NodePrivateKey(deviceID)
	if err != nil {
		return xerrors.Errorf("NodePrivateKey %s,%s", deviceID, err.Error())
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

	deviceInfo.NodeType = api.NodeEdge
	deviceInfo.ExternalIP, _, err = net.SplitHostPort(remoteAddr)
	if err != nil {
		return xerrors.Errorf("SplitHostPort err:%s", err.Error())
	}

	// geoInfo, _ := region.GetRegion().GetGeoInfo(deviceInfo.InternalIP)
	// // TODO Does the error need to be handled?

	// deviceInfo.IPLocation = geoInfo.Geo
	// deviceInfo.Longitude = geoInfo.Longitude
	// deviceInfo.Latitude = geoInfo.Latitude

	edgeNode.BaseInfo = node.NewBaseInfo(&deviceInfo, privateKey, remoteAddr)

	err = s.NodeManager.EdgeOnline(edgeNode)
	if err != nil {
		log.Errorf("EdgeNodeConnect addEdgeNode err:%s,deviceID:%s", err.Error(), deviceInfo.DeviceID)
		return err
	}

	// notify locator
	locator.ChangeNodeOnlineStatus(deviceID, true)

	s.DataSync.Add2List(deviceID)

	return nil
}

// GetPublicKey get node Public Key
func (s *Scheduler) GetPublicKey(ctx context.Context) (string, error) {
	deviceID := handler.GetDeviceID(ctx)

	edgeNode := s.NodeManager.GetEdgeNode(deviceID)
	if edgeNode != nil {
		return titanRsa.PublicKey2Pem(&edgeNode.PrivateKey().PublicKey), nil
	}

	candidateNode := s.NodeManager.GetCandidateNode(deviceID)
	if candidateNode != nil {
		return titanRsa.PublicKey2Pem(&candidateNode.PrivateKey().PublicKey), nil
	}

	return "", fmt.Errorf("Can not get node %s publicKey", deviceID)
}

// GetExternalAddr get node External address
func (s *Scheduler) GetExternalAddr(ctx context.Context) (string, error) {
	remoteAddr := handler.GetRemoteAddr(ctx)
	return remoteAddr, nil
}

// ValidateBlockResult Validator Block Result
func (s *Scheduler) ValidateBlockResult(ctx context.Context, validateResults api.ValidateResults) error {
	validator := handler.GetDeviceID(ctx)
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

// GetOnlineDeviceIDs Get all online node id
func (s *Scheduler) GetOnlineDeviceIDs(ctx context.Context, nodeType api.NodeType) ([]string, error) {
	if nodeType == api.NodeValidate {
		list, err := s.NodeManager.NodeMgrDB.GetValidatorsWithList("Server_ID")
		if err != nil {
			return nil, err
		}

		out := make([]string, 0)
		for _, deviceID := range list {
			node := s.NodeManager.GetCandidateNode(deviceID)
			if node != nil {
				out = append(out, deviceID)
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
func (s *Scheduler) GetDevicesInfo(ctx context.Context, deviceID string) (api.DeviceInfo, error) {
	// node datas
	deviceInfo, err := s.NodeManager.NodeMgrDB.LoadNodeInfo(deviceID)
	if err != nil {
		log.Errorf("getNodeInfo: %s ,deviceID : %s", err.Error(), deviceID)
		return api.DeviceInfo{}, err
	}

	isOnline := s.NodeManager.GetCandidateNode(deviceID) != nil
	if !isOnline {
		isOnline = s.NodeManager.GetEdgeNode(deviceID) != nil
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
func (s *Scheduler) GetDownloadInfo(ctx context.Context, deviceID string) ([]*api.BlockDownloadInfo, error) {
	return s.NodeManager.CarfileDB.GetBlockDownloadInfoByDeviceID(deviceID)
}

// NodeQuit node want to quit titan
func (s *Scheduler) NodeQuit(ctx context.Context, deviceID, secret string) error {
	// TODO Check secret
	s.NodeManager.NodesQuit([]string{deviceID})

	return nil
}

func (s *Scheduler) nodeExitedCallback(deviceIDs []string) {
	// clean node cache
	log.Infof("node event , nodes quit:%v", deviceIDs)

	hashs, err := s.NodeManager.CarfileDB.LoadCarfileRecordsWithNodes(deviceIDs)
	if err != nil {
		log.Errorf("LoadCarfileRecordsWithNodes err:%s", err.Error())
		return
	}

	err = s.NodeManager.CarfileDB.RemoveReplicaInfoWithNodes(deviceIDs)
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
func (s *Scheduler) SetNodePort(ctx context.Context, deviceID, port string) error {
	return s.NodeManager.NodeMgrDB.SetNodePort(deviceID, port)
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
func (s *Scheduler) ShowNodeLogFile(ctx context.Context, deviceID string) (*api.LogFile, error) {
	cNode := s.NodeManager.GetCandidateNode(deviceID)
	if cNode != nil {
		return cNode.API().ShowLogFile(ctx)
	}

	eNode := s.NodeManager.GetEdgeNode(deviceID)
	if eNode != nil {
		return eNode.API().ShowLogFile(ctx)
	}

	return nil, xerrors.Errorf("node %s not found")
}

// DownloadNodeLogFile Download Node Log File
func (s *Scheduler) DownloadNodeLogFile(ctx context.Context, deviceID string) ([]byte, error) {
	cNode := s.NodeManager.GetCandidateNode(deviceID)
	if cNode != nil {
		return cNode.API().DownloadLogFile(ctx)
	}

	eNode := s.NodeManager.GetEdgeNode(deviceID)
	if eNode != nil {
		return eNode.API().DownloadLogFile(ctx)
	}

	return nil, xerrors.Errorf("node %s not found")
}

func (s *Scheduler) DeleteNodeLogFile(ctx context.Context, deviceID string) error {
	cNode := s.NodeManager.GetCandidateNode(deviceID)
	if cNode != nil {
		return cNode.API().DeleteLogFile(ctx)
	}

	eNode := s.NodeManager.GetEdgeNode(deviceID)
	if eNode != nil {
		return eNode.API().DeleteLogFile(ctx)
	}

	return xerrors.Errorf("node %s not found")
}

// deviceExists Check if the id exists
func (s *Scheduler) deviceExists(deviceID string, nodeType int) bool {
	var nType int
	err := s.NodeManager.CarfileDB.GetNodeAllocateInfo(deviceID, persistent.NodeTypeKey, &nType)
	if err != nil {
		return false
	}

	if nodeType != 0 {
		return nType == nodeType
	}

	return true
}
