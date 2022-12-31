package scheduler

import (
	"context"
	"crypto/rsa"
	"fmt"
	"math/rand"
	"net/http"
	"time"

	"github.com/linguohua/titan/node/scheduler/election"
	"github.com/linguohua/titan/node/scheduler/validate"

	// "github.com/linguohua/titan/node/device"

	"github.com/filecoin-project/go-jsonrpc/auth"
	logging "github.com/ipfs/go-log/v2"
	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/api/client"
	"github.com/linguohua/titan/node/common"
	"github.com/linguohua/titan/node/handler"
	"github.com/linguohua/titan/node/helper"
	titanRsa "github.com/linguohua/titan/node/rsa"
	"github.com/linguohua/titan/node/scheduler/area"
	"github.com/linguohua/titan/node/scheduler/node"

	// "github.com/linguohua/titan/node/device"
	"github.com/linguohua/titan/node/repo"
	"github.com/linguohua/titan/node/scheduler/data"
	"github.com/linguohua/titan/node/scheduler/db/cache"
	"github.com/linguohua/titan/node/scheduler/db/persistent"
	"github.com/linguohua/titan/node/scheduler/locator"
	"github.com/linguohua/titan/node/scheduler/sync"
	"github.com/linguohua/titan/node/scheduler/web"

	"github.com/linguohua/titan/node/secret"
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
	blockDownloadStatusSuccess
)

// NewLocalScheduleNode NewLocalScheduleNode
func NewLocalScheduleNode(lr repo.LockedRepo, port int, areaStr string) api.Scheduler {
	s := &Scheduler{}

	nodeManager := node.NewNodeManager(s.nodeOfflineCallback, s.nodeExitedCallback, s.getAuthToken)

	s.locatorManager = locator.NewLoactorManager(port)
	s.nodeManager = nodeManager
	s.election = election.NewElection(nodeManager)
	s.validate = validate.NewValidate(nodeManager, true)
	s.dataManager = data.NewDataManager(nodeManager)
	s.CommonAPI = common.NewCommonAPI(nodeManager.UpdateLastRequestTime)
	s.Web = web.NewWeb(s)
	s.dataSync = sync.NewDataSync()

	sec, err := secret.APISecret(lr)
	if err != nil {
		log.Panicf("NewLocalScheduleNode failed:%s", err.Error())
	}
	s.APISecret = sec

	area.InitServerArea(areaStr)

	err = s.authNew()
	if err != nil {
		log.Panicf("authNew err:%s", err.Error())
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
	dataManager    *data.Manager
	locatorManager *locator.Manager
	// selector       *ValidateSelector
	dataSync *sync.DataSync

	authToken []byte
}

func (s *Scheduler) getAuthToken() []byte {
	return s.authToken
}

// CandidateNodeConnect Candidate connect
func (s *Scheduler) CandidateNodeConnect(ctx context.Context, rpcURL, downloadSrvURL string) error {
	ip := handler.GetRequestIP(ctx)
	deviceID := handler.GetDeviceID(ctx)

	if !isDeviceExist(deviceID, int(api.NodeCandidate)) {
		return xerrors.Errorf("candidate node not Exist: %s", deviceID)
	}

	// url := fmt.Sprintf("http://%s:%d/rpc/v0", ip, port)
	log.Infof("CandidateNodeConnect deviceID:%s, rpc url:%s", deviceID, rpcURL)

	t, err := s.AuthNew(ctx, api.AllPermissions)
	if err != nil {
		return xerrors.Errorf("creating auth token for remote connection: %s", err.Error())
	}

	headers := http.Header{}
	headers.Add("Authorization", "Bearer "+string(t))

	// Connect to node
	candicateAPI, closer, err := client.NewCandicate(ctx, rpcURL, headers)
	if err != nil {
		log.Errorf("CandidateNodeConnect NewCandicate err:%s,url:%s", err.Error(), rpcURL)
		return err
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

	authInfo, _ := persistent.GetDB().GetNodeAuthInfo(deviceID)
	var privateKey *rsa.PrivateKey
	if authInfo != nil && len(authInfo.PrivateKey) > 0 {
		privateKey, err = titanRsa.Pem2PrivateKey(authInfo.PrivateKey)
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
	deviceInfo.ExternalIp = ip

	geoInfo, isOk := area.GetGeoInfoWithIP(deviceInfo.ExternalIp)
	if !isOk {
		log.Errorf("CandidateNodeConnect err DeviceId:%s,ip%s,geo:%s", deviceID, deviceInfo.ExternalIp, geoInfo.Geo)
		return xerrors.New("Area not exist")
	}
	deviceInfo.IpLocation = geoInfo.Geo

	candidateNode := node.NewCandidateNode(candicateAPI, closer, node.NewNode(&deviceInfo, rpcURL, downloadSrvURL, privateKey, api.TypeNameCandidate, geoInfo))

	err = s.nodeManager.CandidateOnline(candidateNode)
	if err != nil {
		log.Errorf("CandidateNodeConnect addEdgeNode err:%s,deviceID:%s", err.Error(), deviceID)
		return err
	}

	err = candidateNode.SaveInfo()
	if err != nil {
		log.Errorf("CandidateNodeConnect SaveInfo err:%s,deviceID:%s", err.Error(), deviceID)
		return err
	}

	go s.locatorManager.NotifyNodeStatusToLocator(deviceID, true)

	s.dataSync.Add2List(candicateAPI, deviceID)

	return nil
}

// EdgeNodeConnect edge connect
func (s *Scheduler) EdgeNodeConnect(ctx context.Context, rpcURL, downloadSrvURL string) error {
	ip := handler.GetRequestIP(ctx)
	deviceID := handler.GetDeviceID(ctx)

	if !isDeviceExist(deviceID, int(api.NodeEdge)) {
		return xerrors.Errorf("edge node not Exist: %s", deviceID)
	}

	// url := fmt.Sprintf("http://%s:%d/rpc/v0", ip, port)
	log.Infof("EdgeNodeConnect %s ;ip:%s", deviceID, ip)

	t, err := s.AuthNew(ctx, api.AllPermissions)
	if err != nil {
		return xerrors.Errorf("creating auth token for remote connection: %s", err.Error())
	}

	headers := http.Header{}
	headers.Add("Authorization", "Bearer "+string(t))

	// Connect to node
	edgeAPI, closer, err := client.NewEdge(ctx, rpcURL, headers)
	if err != nil {
		log.Errorf("EdgeNodeConnect NewEdge err:%s,url:%s", err.Error(), rpcURL)
		return err
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

	authInfo, _ := persistent.GetDB().GetNodeAuthInfo(deviceID)
	var privateKey *rsa.PrivateKey
	if authInfo != nil && len(authInfo.PrivateKey) > 0 {
		privateKey, err = titanRsa.Pem2PrivateKey(authInfo.PrivateKey)
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
	deviceInfo.ExternalIp = ip

	geoInfo, isOk := area.GetGeoInfoWithIP(deviceInfo.ExternalIp)
	if !isOk {
		log.Errorf("edgeOnline err DeviceId:%s,ip%s,geo:%s", deviceID, deviceInfo.ExternalIp, geoInfo.Geo)
		return xerrors.New("Area not exist")
	}

	deviceInfo.IpLocation = geoInfo.Geo

	edgeNode := node.NewEdgeNode(edgeAPI, closer, node.NewNode(&deviceInfo, rpcURL, downloadSrvURL, privateKey, api.TypeNameEdge, geoInfo))

	err = s.nodeManager.EdgeOnline(edgeNode)
	if err != nil {
		log.Errorf("EdgeNodeConnect addEdgeNode err:%s,deviceID:%s", err.Error(), deviceInfo.DeviceId)
		return err
	}

	err = edgeNode.SaveInfo()
	if err != nil {
		log.Errorf("EdgeNodeConnect set device info: %s", err.Error())
		return err
	}

	// notify locator
	go s.locatorManager.NotifyNodeStatusToLocator(deviceID, true)

	s.dataSync.Add2List(edgeAPI, deviceID)

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

// GetExternalIP get node External IP
func (s *Scheduler) GetExternalIP(ctx context.Context) (string, error) {
	return handler.GetRequestIP(ctx), nil
}

// ValidateBlockResult Validate Block Result
func (s *Scheduler) ValidateBlockResult(ctx context.Context, validateResults api.ValidateResults) error {
	validator := handler.GetDeviceID(ctx)
	log.Debug("call back validate block result, validator is", validator)
	if !isDeviceExist(validator, 0) {
		return xerrors.Errorf("node not Exist: %s", validator)
	}

	vs := &validateResults
	vs.Validator = validator

	s.validate.PushResultToQueue(vs)
	return nil
}

// RegisterNode Register Node
func (s *Scheduler) RegisterNode(ctx context.Context, nodeType api.NodeType, count int) ([]api.NodeRegisterInfo, error) {
	list := make([]api.NodeRegisterInfo, 0)
	if count <= 0 {
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

// // GetToken get token
// func (s *Scheduler) GetToken(ctx context.Context, deviceID, secret string) (string, error) {
// 	return generateToken(deviceID, secret)
// }

// GetOnlineDeviceIDs Get all online node id
func (s *Scheduler) GetOnlineDeviceIDs(ctx context.Context, nodeType api.NodeTypeName) ([]string, error) {
	if nodeType == api.TypeNameValidator {
		return cache.GetDB().GetValidatorsWithList()
	}

	return s.nodeManager.GetNodes(nodeType)
}

// GetCandidateDownloadInfoWithBlocks find node
func (s *Scheduler) GetCandidateDownloadInfoWithBlocks(ctx context.Context, cids []string) (map[string]api.CandidateDownloadInfo, error) {
	deviceID := handler.GetDeviceID(ctx)

	if !isDeviceExist(deviceID, 0) {
		return nil, xerrors.Errorf("node not Exist: %s", deviceID)
	}

	if len(cids) < 1 {
		return nil, xerrors.New("cids is nil")
	}

	tk := s.getAuthToken()

	infoMap := make(map[string]api.CandidateDownloadInfo)

	for _, cid := range cids {
		hash, err := helper.CIDString2HashString(cid)
		if err != nil {
			continue
		}

		infos, err := s.nodeManager.GetCandidatesWithBlockHash(hash, deviceID)
		if err != nil || len(infos) <= 0 {
			continue
		}

		candidate := infos[randomNum(0, len(infos))]
		infoMap[cid] = api.CandidateDownloadInfo{URL: candidate.GetAddress(), Token: string(tk)}
	}

	return infoMap, nil
}

// QueryCacheStatWithNode Query Cache Stat
func (s *Scheduler) QueryCacheStatWithNode(ctx context.Context, deviceID string) ([]api.CacheStat, error) {
	statList := make([]api.CacheStat, 0)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// node datas
	candidata := s.nodeManager.GetCandidateNode(deviceID)
	if candidata != nil {
		// redis datas
		body := api.CacheStat{}
		count, err := persistent.GetDB().CountCidOfDevice(deviceID)
		if err == nil {
			body.CacheBlockCount = int(count)
		}

		statList = append(statList, body)

		nodeBody, _ := candidata.GetAPI().QueryCacheStat(ctx)
		statList = append(statList, nodeBody)
		return statList, nil
	}

	edge := s.nodeManager.GetEdgeNode(deviceID)
	if edge != nil {
		body := api.CacheStat{}
		count, err := persistent.GetDB().CountCidOfDevice(deviceID)
		if err == nil {
			body.CacheBlockCount = int(count)
		}

		statList = append(statList, body)

		nodeBody, _ := edge.GetAPI().QueryCacheStat(ctx)
		statList = append(statList, nodeBody)
		return statList, nil
	}

	return statList, xerrors.Errorf("not found node:%s", deviceID)
}

// QueryCachingBlocksWithNode Query Caching Blocks
func (s *Scheduler) QueryCachingBlocksWithNode(ctx context.Context, deviceID string) (api.CachingBlockList, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	candidata := s.nodeManager.GetCandidateNode(deviceID)
	if candidata != nil {
		return candidata.GetAPI().QueryCachingBlocks(ctx)
	}

	edge := s.nodeManager.GetEdgeNode(deviceID)
	if edge != nil {
		return edge.GetAPI().QueryCachingBlocks(ctx)
	}

	return api.CachingBlockList{}, xerrors.Errorf("not found node:%s", deviceID)
}

// ElectionValidators Validators
func (s *Scheduler) ElectionValidators(ctx context.Context) error {
	s.election.Start()
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
func (s *Scheduler) ValidateSwitch(ctx context.Context, open bool) error {
	s.validate.EnableValidate(open)
	return nil
}

// ValidateRunningState get validate running state,
// false is close
// true is open
func (s *Scheduler) ValidateRunningState(ctx context.Context) (bool, error) {
	// the framework requires that the method must return error
	return s.validate.IsEnable(), nil
}

func (s *Scheduler) ValidateStart(ctx context.Context) error {
	return s.validate.StartValidateOnceTask()
}

// LocatorConnect Locator Connect
func (s *Scheduler) LocatorConnect(ctx context.Context, port int, areaID, locatorID string, locatorToken string) error {
	if !area.IsExist(areaID) {
		return xerrors.Errorf("area err:%s", areaID)
	}

	ip := handler.GetRequestIP(ctx)
	url := fmt.Sprintf("http://%s:%d/rpc/v0", ip, port)
	log.Infof("LocatorConnect locatorID:%s,areaID:%s,LocatorConnect ip:%s,port:%d", locatorID, areaID, ip, port)

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

// NodeExit node want to exit titan
func (s *Scheduler) NodeExit(ctx context.Context, deviceID string) error {
	s.nodeManager.NodeQuit(deviceID)

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

// func incrDeviceBlock(deviceID string, blocks int) error {
// 	return cache.GetDB().UpdateDeviceInfo(deviceID, func(deviceInfo *api.DevicesInfo) {
// 		deviceInfo.BlockCount += blocks
// 	})
// }

func (s *Scheduler) nodeOfflineCallback(deviceID string) {
	go s.locatorManager.NotifyNodeStatusToLocator(deviceID, false)
}

func (s *Scheduler) nodeExitedCallback(deviceID string) {
	// clean node cache
	s.dataManager.CleanNodeAndRestoreCaches(deviceID)
}

//RedressDeveiceInfo redress device info
func (s *Scheduler) RedressDeveiceInfo(ctx context.Context, deviceID string) error {
	if !isDeviceExist(deviceID, 0) {
		return xerrors.Errorf("node not Exist: %s", deviceID)
	}

	blocks, err := persistent.GetDB().GetBlocksFID(deviceID)
	if err != nil {
		return err
	}

	err = cache.GetDB().UpdateDeviceInfo(deviceID, "BlockCount", len(blocks))
	if err != nil {
		return err
	}

	return nil
}

func (s *Scheduler) authNew() error {
	tk, err := s.AuthNew(context.Background(), []auth.Permission{api.PermRead, api.PermWrite})
	if err != nil {
		log.Errorf("findDownloadinfoForBlocks AuthNew err:%s", err.Error())
		return err
	}

	s.authToken = tk

	return nil
}

// isDeviceExist Check if the id exists
func isDeviceExist(deviceID string, nodeType int) bool {
	var nType int
	err := persistent.GetDB().GetRegisterInfo(deviceID, "node_type", &nType)
	if err != nil {
		return false
	}

	if nodeType != 0 {
		return nType == nodeType
	}

	return true
}
