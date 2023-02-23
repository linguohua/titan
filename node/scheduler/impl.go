package scheduler

import (
	"context"
	"crypto/rsa"
	"fmt"
	"math/rand"
	"net"
	"net/http"
	"time"

	"github.com/gbrlsnchs/jwt/v3"
	"github.com/linguohua/titan/node/config"
	"github.com/linguohua/titan/node/scheduler/election"
	"github.com/linguohua/titan/node/scheduler/validate"
	"github.com/linguohua/titan/node/secret"
	"github.com/linguohua/titan/region"

	// "github.com/linguohua/titan/node/device"

	"github.com/filecoin-project/go-jsonrpc/auth"
	logging "github.com/ipfs/go-log/v2"
	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/api/client"
	"github.com/linguohua/titan/node/common"
	"github.com/linguohua/titan/node/handler"
	titanRsa "github.com/linguohua/titan/node/rsa"
	"github.com/linguohua/titan/node/scheduler/node"

	// "github.com/linguohua/titan/node/device"
	"github.com/linguohua/titan/node/repo"
	"github.com/linguohua/titan/node/scheduler/carfile"
	"github.com/linguohua/titan/node/scheduler/db/cache"
	"github.com/linguohua/titan/node/scheduler/db/persistent"
	"github.com/linguohua/titan/node/scheduler/locator"
	"github.com/linguohua/titan/node/scheduler/sync"
	"github.com/linguohua/titan/node/scheduler/web"

	"golang.org/x/xerrors"
)

var (
	log    = logging.Logger("scheduler")
	myRand = rand.New(rand.NewSource(time.Now().UnixNano()))
)

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

// NewLocalScheduleNode NewLocalScheduleNode
func NewLocalScheduleNode(lr repo.LockedRepo, schedulerCfg *config.SchedulerCfg) api.Scheduler {
	s := &Scheduler{}

	nodeManager := node.NewNodeManager(s.nodeOfflineCallback, s.nodeExitedCallback)
	s.CommonAPI = common.NewCommonAPI(nodeManager.NodeSessionCallBack)
	s.Web = web.NewWeb(s)

	sec, err := secret.APISecret(lr)
	if err != nil {
		log.Panicf("NewLocalScheduleNode failed:%s", err.Error())
	}
	s.APISecret = sec

	// area.InitServerArea(areaStr)

	err = s.authNew()
	if err != nil {
		log.Panicf("authNew err:%s", err.Error())
	}

	s.locatorManager = locator.NewLoactorManager()
	s.nodeManager = nodeManager
	s.election = election.NewElection(nodeManager)
	s.validate = validate.NewValidate(nodeManager, false)
	s.dataManager = carfile.NewCarfileManager(nodeManager, s.writeToken)
	s.dataSync = sync.NewDataSync(nodeManager)
	s.schedulerCfg = schedulerCfg

	s.nodeAppUpdateInfos, err = persistent.GetDB().GetNodeUpdateInfos()
	if err != nil {
		log.Errorf("GetNodeUpdateInfos error:%s", err)
	}

	return s
}

// Scheduler node
type Scheduler struct {
	common.CommonAPI
	api.Web

	nodeManager    *node.Manager
	election       *election.Election
	validate       *validate.Validate
	dataManager    *carfile.Manager
	locatorManager *locator.Manager
	dataSync       *sync.DataSync

	writeToken         []byte
	adminToken         []byte
	nodeAppUpdateInfos map[int]*api.NodeAppUpdateInfo
	schedulerCfg       *config.SchedulerCfg
}

type jwtPayload struct {
	Allow []auth.Permission
}

// AuthNodeVerify Verify Node Auth
func (s *Scheduler) AuthNodeVerify(ctx context.Context, token string) ([]auth.Permission, error) {
	var payload jwtPayload

	deviceID := handler.GetDeviceID(ctx)
	// fmt.Println("AuthNodeVerify device id :", deviceID)
	if deviceID == "" {
		if _, err := jwt.Verify([]byte(token), (*jwt.HMACSHA)(s.APISecret), &payload); err != nil {
			return nil, xerrors.Errorf("JWT Verification failed: %w", err)
		}

		return payload.Allow, nil
	}

	var secret string
	err := persistent.GetDB().GetRegisterInfo(deviceID, persistent.SecretKey, &secret)
	if err != nil {
		return nil, xerrors.Errorf("JWT Verification %s GetRegisterInfo failed: %w", deviceID, err)
	}

	deviceSecret := secret
	// fmt.Println("AuthNodeVerify deviceSecret:", deviceSecret)

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
	err := persistent.GetDB().GetRegisterInfo(deviceID, persistent.SecretKey, &secret)
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

	if !deviceExists(deviceID, int(api.NodeCandidate)) {
		return xerrors.Errorf("candidate node not Exist: %s", deviceID)
	}

	log.Infof("Candidate Connect %s, address:%s", deviceID, remoteAddr)
	candidateNode := node.NewCandidateNode(s.adminToken)
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

	if deviceID != deviceInfo.DeviceId {
		return xerrors.Errorf("deviceID mismatch %s,%s", deviceID, deviceInfo.DeviceId)
	}

	privateKeyStr, _ := persistent.GetDB().GetNodePrivateKey(deviceID)
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
	deviceInfo.ExternalIp, _, _ = net.SplitHostPort(remoteAddr)

	geoInfo, _ := region.GetRegion().GetGeoInfo(deviceInfo.ExternalIp)
	// TODO Does the error need to be handled?

	deviceInfo.IpLocation = geoInfo.Geo
	deviceInfo.Longitude = geoInfo.Longitude
	deviceInfo.Latitude = geoInfo.Latitude

	candidateNode.Node = node.NewNode(&deviceInfo, remoteAddr, privateKey, api.TypeNameCandidate, geoInfo)

	err = s.nodeManager.CandidateOnline(candidateNode)
	if err != nil {
		log.Errorf("CandidateNodeConnect addEdgeNode err:%s,deviceID:%s", err.Error(), deviceID)
		return err
	}

	err = candidateNode.SaveInfo(&deviceInfo)
	if err != nil {
		log.Errorf("CandidateNodeConnect SaveInfo err:%s,deviceID:%s", err.Error(), deviceID)
		return err
	}

	go s.locatorManager.NotifyNodeStatusToLocator(deviceID, true)

	s.dataSync.Add2List(deviceID)

	return nil
}

// EdgeNodeConnect edge connect
func (s *Scheduler) EdgeNodeConnect(ctx context.Context) error {
	remoteAddr := handler.GetRemoteAddr(ctx)
	deviceID := handler.GetDeviceID(ctx)

	if !deviceExists(deviceID, int(api.NodeEdge)) {
		return xerrors.Errorf("edge node not Exist: %s", deviceID)
	}

	log.Infof("Edge Connect %s; remoteAddr:%s", deviceID, remoteAddr)
	edgeNode := node.NewEdgeNode(s.adminToken)
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

	if deviceID != deviceInfo.DeviceId {
		return xerrors.Errorf("deviceID mismatch %s,%s", deviceID, deviceInfo.DeviceId)
	}

	privateKeyStr, _ := persistent.GetDB().GetNodePrivateKey(deviceID)
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
	deviceInfo.ExternalIp, _, _ = net.SplitHostPort(remoteAddr)

	geoInfo, _ := region.GetRegion().GetGeoInfo(deviceInfo.ExternalIp)
	// TODO Does the error need to be handled?

	deviceInfo.IpLocation = geoInfo.Geo
	deviceInfo.Longitude = geoInfo.Longitude
	deviceInfo.Latitude = geoInfo.Latitude

	edgeNode.Node = node.NewNode(&deviceInfo, remoteAddr, privateKey, api.TypeNameEdge, geoInfo)

	err = s.nodeManager.EdgeOnline(edgeNode)
	if err != nil {
		log.Errorf("EdgeNodeConnect addEdgeNode err:%s,deviceID:%s", err.Error(), deviceInfo.DeviceId)
		return err
	}

	err = edgeNode.SaveInfo(&deviceInfo)
	if err != nil {
		log.Errorf("EdgeNodeConnect set device info: %s", err.Error())
		return err
	}

	// notify locator
	go s.locatorManager.NotifyNodeStatusToLocator(deviceID, true)

	s.dataSync.Add2List(deviceID)

	return nil
}

// GetPublicKey get node Public Key
func (s *Scheduler) GetPublicKey(ctx context.Context) (string, error) {
	deviceID := handler.GetDeviceID(ctx)

	edgeNode := s.nodeManager.GetEdgeNode(deviceID)
	if edgeNode != nil {
		return titanRsa.PublicKey2Pem(&edgeNode.GetPrivateKey().PublicKey), nil
	}

	candidateNode := s.nodeManager.GetCandidateNode(deviceID)
	if candidateNode != nil {
		return titanRsa.PublicKey2Pem(&candidateNode.GetPrivateKey().PublicKey), nil
	}

	return "", fmt.Errorf("Can not get node %s publicKey", deviceID)
}

// GetExternalAddr get node External address
func (s *Scheduler) GetExternalAddr(ctx context.Context) (string, error) {
	remoteAddr := handler.GetRemoteAddr(ctx)
	return remoteAddr, nil
}

// ValidateBlockResult Validate Block Result
func (s *Scheduler) ValidateBlockResult(ctx context.Context, validateResults api.ValidateResults) error {
	validator := handler.GetDeviceID(ctx)
	log.Debug("call back validate block result, validator is", validator)
	if !deviceExists(validator, 0) {
		return xerrors.Errorf("node not Exist: %s", validator)
	}

	vs := &validateResults
	vs.Validator = validator

	// s.validate.PushResultToQueue(vs)
	s.validate.ValidateResult(vs)
	return nil
}

// RegisterNode Register Node
func (s *Scheduler) RegisterNode(ctx context.Context, nodeType api.NodeType, count int) ([]api.NodeRegisterInfo, error) {
	list := make([]api.NodeRegisterInfo, 0)
	if count <= 0 || count > 10 {
		return list, nil
	}

	for i := 0; i < count; i++ {
		info, err := node.RegisterNode(nodeType)
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
		list, err := cache.GetDB().GetValidatorsWithList()
		if err != nil {
			return nil, err
		}

		out := make([]string, 0)
		for _, deviceID := range list {
			node := s.nodeManager.GetCandidateNode(deviceID)
			if node != nil {
				out = append(out, deviceID)
			}
		}
		return out, nil
	}

	return s.nodeManager.GetOnlineNodes(nodeType)
}

// ElectionValidators Validators
func (s *Scheduler) ElectionValidators(ctx context.Context) error {
	s.election.StartElect()
	return nil
}

// GetDevicesInfo return the devices information
func (s *Scheduler) GetDevicesInfo(ctx context.Context, deviceID string) (api.DevicesInfo, error) {
	// node datas
	deviceInfo, err := cache.GetDB().GetDeviceInfo(deviceID)
	if err != nil {
		log.Errorf("getNodeInfo: %s ,deviceID : %s", err.Error(), deviceID)
		return api.DevicesInfo{}, err
	}

	isOnline := s.nodeManager.GetCandidateNode(deviceID) != nil
	if !isOnline {
		isOnline = s.nodeManager.GetEdgeNode(deviceID) != nil
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

func randomNum(start, end int) int {
	max := end - start
	if max <= 0 {
		return start
	}

	x := myRand.Intn(10000)
	y := x % end

	return y + start
}

// ValidateSwitch open or close validate task
func (s *Scheduler) ValidateSwitch(ctx context.Context, enable bool) error {
	s.validate.EnableValidate(enable)
	return nil
}

// ValidateRunningState get validate running state,
// false is close
// true is open
func (s *Scheduler) ValidateRunningState(ctx context.Context) (bool, error) {
	// the framework requires that the method must return error
	return s.validate.IsEnable(), nil
}

// ValidateStart start once validate
func (s *Scheduler) ValidateStart(ctx context.Context) error {
	return s.validate.StartValidateOnceTask()
}

// LocatorConnect Locator Connect
func (s *Scheduler) LocatorConnect(ctx context.Context, locatorID string, locatorToken string) error {
	remoteAddr := handler.GetRemoteAddr(ctx)
	url := fmt.Sprintf("https://%s/rpc/v0", remoteAddr)

	log.Infof("LocatorConnect locatorID:%s, addr:%s", locatorID, remoteAddr)

	headers := http.Header{}
	headers.Add("Authorization", "Bearer "+string(locatorToken))
	// Connect to scheduler
	// log.Infof("EdgeNodeConnect edge url:%v", url)
	locationAPI, closer, err := client.NewLocator(ctx, url, headers)
	if err != nil {
		log.Errorf("LocatorConnect err:%s,url:%s", err.Error(), url)
		return err
	}

	s.locatorManager.AddLocator(node.NewLocation(locationAPI, closer, locatorID))

	return nil
}

// GetDownloadInfo get node download info
func (s *Scheduler) GetDownloadInfo(ctx context.Context, deviceID string) ([]*api.BlockDownloadInfo, error) {
	return persistent.GetDB().GetBlockDownloadInfoByDeviceID(deviceID)
}

// NodeQuit node want to quit titan
func (s *Scheduler) NodeQuit(ctx context.Context, deviceID string) error {
	s.nodeManager.NodesQuit([]string{deviceID})

	return nil
}

func incrDeviceReward(deviceID string, incrReward int64) error {
	err := cache.GetDB().IncrByDeviceInfo(deviceID, "CumulativeProfit", incrReward)
	if err != nil {
		log.Errorf("IncrByDeviceInfo err:%s ", err.Error())
		return err
	}

	return nil
}

func (s *Scheduler) nodeOfflineCallback(deviceID string) {
	s.locatorManager.NotifyNodeStatusToLocator(deviceID, false)
}

func (s *Scheduler) nodeExitedCallback(deviceIDs []string) {
	// clean node cache
	s.dataManager.NodesQuit(deviceIDs)
}

func (s *Scheduler) authNew() error {
	wtk, err := s.AuthNew(context.Background(), []auth.Permission{api.PermRead, api.PermWrite})
	if err != nil {
		log.Errorf("AuthNew err:%s", err.Error())
		return err
	}

	s.writeToken = wtk

	atk, err := s.AuthNew(context.Background(), api.AllPermissions)
	if err != nil {
		log.Errorf("AuthNew err:%s", err.Error())
		return err
	}

	s.adminToken = atk

	return nil
}

// ShowNodeLogFile show node log file
func (s *Scheduler) ShowNodeLogFile(ctx context.Context, deviceID string) (*api.LogFile, error) {
	cNode := s.nodeManager.GetCandidateNode(deviceID)
	if cNode != nil {
		return cNode.GetAPI().ShowLogFile(ctx)
	}

	eNode := s.nodeManager.GetEdgeNode(deviceID)
	if eNode != nil {
		return eNode.GetAPI().ShowLogFile(ctx)
	}

	return nil, xerrors.Errorf("node %s not found")
}

// DownloadNodeLogFile Download Node Log File
func (s *Scheduler) DownloadNodeLogFile(ctx context.Context, deviceID string) ([]byte, error) {
	cNode := s.nodeManager.GetCandidateNode(deviceID)
	if cNode != nil {
		return cNode.GetAPI().DownloadLogFile(ctx)
	}

	eNode := s.nodeManager.GetEdgeNode(deviceID)
	if eNode != nil {
		return eNode.GetAPI().DownloadLogFile(ctx)
	}

	return nil, xerrors.Errorf("node %s not found")
}

func (s *Scheduler) DeleteNodeLogFile(ctx context.Context, deviceID string) error {
	cNode := s.nodeManager.GetCandidateNode(deviceID)
	if cNode != nil {
		return cNode.GetAPI().DeleteLogFile(ctx)
	}

	eNode := s.nodeManager.GetEdgeNode(deviceID)
	if eNode != nil {
		return eNode.GetAPI().DeleteLogFile(ctx)
	}

	return xerrors.Errorf("node %s not found")
}

// deviceExists Check if the id exists
func deviceExists(deviceID string, nodeType int) bool {
	var nType int
	err := persistent.GetDB().GetRegisterInfo(deviceID, persistent.NodeTypeKey, &nType)
	if err != nil {
		return false
	}

	if nodeType != 0 {
		return nType == nodeType
	}

	return true
}
