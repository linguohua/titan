package scheduler

import (
	"context"
	"crypto/rsa"
	"fmt"
	"math/rand"
	"net/http"
	"time"

	"github.com/linguohua/titan/node/scheduler/election"
	validate2 "github.com/linguohua/titan/node/scheduler/validate"

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
	"github.com/linguohua/titan/node/scheduler/errmsg"
	"github.com/linguohua/titan/node/scheduler/node"

	// "github.com/linguohua/titan/node/device"
	"github.com/linguohua/titan/node/repo"
	"github.com/linguohua/titan/node/scheduler/data"
	"github.com/linguohua/titan/node/scheduler/db/cache"
	"github.com/linguohua/titan/node/scheduler/db/persistent"
	"github.com/linguohua/titan/node/scheduler/locator"
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
	// verifiedNodeMax := 10

	s := &Scheduler{serverPort: port}

	locatorManager := locator.NewLoactorManager(port)
	// pool := newValidatePool(verifiedNodeMax)
	nodeManager := node.NewNodeManager(s.nodeOfflineCallback, s.nodeExitedCallback, s.authNew)

	ele := election.NewElection(nodeManager)
	validate := validate2.NewValidate(nodeManager)

	dataManager := data.NewDataManager(nodeManager)

	s.locatorManager = locatorManager
	s.nodeManager = nodeManager
	s.election = ele
	s.validate = validate
	s.dataManager = dataManager
	s.CommonAPI = common.NewCommonAPI(nodeManager.UpdateLastRequestTime)

	s.Web = web.NewWeb(s)

	sec, err := secret.APISecret(lr)
	if err != nil {
		log.Panicf("NewLocalScheduleNode failed:%s", err.Error())
	}
	s.APISecret = sec

	area.InitServerArea(areaStr)

	return s
}

// Scheduler node
type Scheduler struct {
	common.CommonAPI
	api.Web

	nodeManager    *node.Manager
	election       *election.Election
	validate       *validate2.Validate
	dataManager    *data.Manager
	locatorManager *locator.Manager
	// selector       *ValidateSelector

	serverPort int
}

// CandidateNodeConnect Candidate connect
func (s *Scheduler) CandidateNodeConnect(ctx context.Context, rpcURL, downloadSrvURL string) error {
	ip := handler.GetRequestIP(ctx)
	deviceID := handler.GetDeviceID(ctx)

	if !s.nodeManager.IsDeviceExist(deviceID, int(api.NodeCandidate)) {
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

	candidateNode := &node.CandidateNode{
		NodeAPI: candicateAPI,
		Closer:  closer,

		Node: node.Node{
			Addr:           rpcURL,
			DeviceInfo:     deviceInfo,
			DownloadSrvURL: downloadSrvURL,
			PrivateKey:     privateKey,
		},
	}

	err = s.nodeManager.CandidateOnline(candidateNode)
	if err != nil {
		log.Errorf("CandidateNodeConnect addEdgeNode err:%s,deviceID:%s", err.Error(), deviceID)
		return err
	}

	deviceInfo.IpLocation = candidateNode.GeoInfo.Geo
	err = s.nodeManager.SetDeviceInfo(deviceID, deviceInfo)
	if err != nil {
		log.Errorf("CandidateNodeConnect setDeviceInfo err:%s,deviceID:%s", err.Error(), deviceID)
		return err
	}

	s.locatorManager.NotifyNodeStatusToLocator(deviceID, true)

	go doDataSync(candicateAPI, deviceID)

	return nil
}

// EdgeNodeConnect edge connect
func (s *Scheduler) EdgeNodeConnect(ctx context.Context, rpcURL, downloadSrvURL string) error {
	ip := handler.GetRequestIP(ctx)
	deviceID := handler.GetDeviceID(ctx)

	if !s.nodeManager.IsDeviceExist(deviceID, int(api.NodeEdge)) {
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

	edgeNode := &node.EdgeNode{
		NodeAPI: edgeAPI,
		Closer:  closer,

		Node: node.Node{
			Addr:           rpcURL,
			DeviceInfo:     deviceInfo,
			DownloadSrvURL: downloadSrvURL,
			PrivateKey:     privateKey,
		},
	}

	err = s.nodeManager.EdgeOnline(edgeNode)
	if err != nil {
		log.Errorf("EdgeNodeConnect addEdgeNode err:%s,deviceID:%s", err.Error(), deviceInfo.DeviceId)
		return err
	}

	deviceInfo.IpLocation = edgeNode.GeoInfo.Geo
	err = s.nodeManager.SetDeviceInfo(deviceID, deviceInfo)
	if err != nil {
		log.Errorf("EdgeNodeConnect set device info: %s", err.Error())
		return err
	}

	// notify locator
	s.locatorManager.NotifyNodeStatusToLocator(deviceID, true)

	go doDataSync(edgeAPI, deviceID)

	return nil
}

// GetPublicKey get node Public Key
func (s *Scheduler) GetPublicKey(ctx context.Context) (string, error) {
	deviceID := handler.GetDeviceID(ctx)

	edgeNode := s.nodeManager.GetEdgeNode(deviceID)
	if edgeNode != nil {
		return titanRsa.PublicKey2Pem(&edgeNode.PrivateKey.PublicKey), nil
	}

	candidateNode := s.nodeManager.GetCandidateNode(deviceID)
	if candidateNode != nil {
		return titanRsa.PublicKey2Pem(&candidateNode.PrivateKey.PublicKey), nil
	}

	return "", fmt.Errorf("Can not get node %s publicKey", deviceID)
}

// GetExternalIP get node External IP
func (s *Scheduler) GetExternalIP(ctx context.Context) (string, error) {
	return handler.GetRequestIP(ctx), nil
}

// ValidateBlockResult Validate Block Result
func (s *Scheduler) ValidateBlockResult(ctx context.Context, validateResults api.ValidateResults) error {
	deviceID := handler.GetDeviceID(ctx)

	if !s.nodeManager.IsDeviceExist(deviceID, 0) {
		return xerrors.Errorf("node not Exist: %s", deviceID)
	}

	vs := &validateResults
	vs.DeviceID = deviceID

	err := s.validate.PushResultToQueue(vs)
	if err != nil {
		log.Errorf("ValidateBlockResult err:%s", err.Error())
		return err
	}

	return nil
}

// RegisterNode Register Node
func (s *Scheduler) RegisterNode(ctx context.Context, nodeType api.NodeType, count int) ([]api.NodeRegisterInfo, error) {
	list := make([]api.NodeRegisterInfo, 0)
	if count <= 0 {
		return list, nil
	}

	for i := 0; i <= count; i++ {
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
		// return s.validatePool.veriftorList, nil
	}

	return s.nodeManager.GetNodes(nodeType)
}

// GetCandidateDownloadInfoWithBlocks find node
func (s *Scheduler) GetCandidateDownloadInfoWithBlocks(ctx context.Context, cids []string) (map[string]api.DownloadInfoResult, error) {
	deviceID := handler.GetDeviceID(ctx)

	if !s.nodeManager.IsDeviceExist(deviceID, 0) {
		return nil, xerrors.Errorf("node not Exist: %s", deviceID)
	}

	if len(cids) < 1 {
		return nil, xerrors.New("cids is nil")
	}

	infoMap := make(map[string]api.DownloadInfoResult)

	for _, cid := range cids {
		hash, err := helper.CIDString2HashString(cid)
		if err != nil {
			continue
		}

		infos, err := s.nodeManager.GetCandidateNodesWithData(hash, deviceID)
		if err != nil || len(infos) <= 0 {
			continue
		}

		candidate := infos[randomNum(0, len(infos))]

		info, err := persistent.GetDB().GetNodeAuthInfo(candidate.DeviceInfo.DeviceId)
		if err != nil {
			continue
		}

		// TODO: complete downloadInfo

		infoMap[cid] = api.DownloadInfoResult{URL: info.URL}
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
		count, err := persistent.GetDB().GetDeviceBlockNum(deviceID)
		if err == nil {
			body.CacheBlockCount = int(count)
		}

		statList = append(statList, body)

		nodeBody, _ := candidata.NodeAPI.QueryCacheStat(ctx)
		statList = append(statList, nodeBody)
		return statList, nil
	}

	edge := s.nodeManager.GetEdgeNode(deviceID)
	if edge != nil {
		body := api.CacheStat{}
		count, err := persistent.GetDB().GetDeviceBlockNum(deviceID)
		if err == nil {
			body.CacheBlockCount = int(count)
		}

		statList = append(statList, body)

		nodeBody, _ := edge.NodeAPI.QueryCacheStat(ctx)
		statList = append(statList, nodeBody)
		return statList, nil
	}

	return statList, xerrors.Errorf("%s:%s", errmsg.ErrNodeNotFind, deviceID)
}

// QueryCachingBlocksWithNode Query Caching Blocks
func (s *Scheduler) QueryCachingBlocksWithNode(ctx context.Context, deviceID string) (api.CachingBlockList, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	candidata := s.nodeManager.GetCandidateNode(deviceID)
	if candidata != nil {
		return candidata.NodeAPI.QueryCachingBlocks(ctx)
	}

	edge := s.nodeManager.GetEdgeNode(deviceID)
	if edge != nil {
		return edge.NodeAPI.QueryCachingBlocks(ctx)
	}

	return api.CachingBlockList{}, xerrors.Errorf("%s:%s", errmsg.ErrNodeNotFind, deviceID)
}

// ElectionValidators Validators
func (s *Scheduler) ElectionValidators(ctx context.Context) error {
	// return s.election.startElection()
	return nil
}

// Validate Validate edge
func (s *Scheduler) Validate(ctx context.Context) error {
	// return s.validate.startValidate()
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

	return deviceInfo, nil
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
	// s.validate.open = open
	return nil
}

// LocatorConnect Locator Connect
func (s *Scheduler) LocatorConnect(ctx context.Context, port int, areaID, locatorID string, locatorToken string) error {
	ip := handler.GetRequestIP(ctx)
	url := fmt.Sprintf("http://%s:%d/rpc/v0", ip, port)
	log.Infof("LocatorConnect locatorID:%s,areaID:%s,LocatorConnect ip:%s,port:%d", locatorID, areaID, ip, port)

	if areaID != area.ServerArea {
		return xerrors.Errorf("area err:%s", areaID)
	}

	headers := http.Header{}
	headers.Add("Authorization", "Bearer "+string(locatorToken))
	// Connect to scheduler
	// log.Infof("EdgeNodeConnect edge url:%v", url)
	locationAPI, closer, err := client.NewLocator(ctx, url, headers)
	if err != nil {
		log.Errorf("LocatorConnect err:%s,url:%s", err.Error(), url)
		return err
	}

	s.locatorManager.AddLocator(&node.Location{LocatorID: locatorID, NodeAPI: locationAPI, Closer: closer})

	return nil
}

// GetDownloadInfo get node download info
func (s *Scheduler) GetDownloadInfo(ctx context.Context, deviceID string) ([]*api.BlockDownloadInfo, error) {
	return persistent.GetDB().GetBlockDownloadInfoByDeviceID(deviceID)
}

// GetValidationInfo Get Validation Info
func (s *Scheduler) GetValidationInfo(ctx context.Context) (api.ValidationInfo, error) {
	// nextElectionTime := s.selector.getNextElectionTime()
	// isEnable := s.validate.open

	// validators, err := cache.GetDB().GetValidatorsWithList()
	// if err != nil {
	// 	return api.ValidationInfo{}, err
	// }

	// return api.ValidationInfo{Validators: validators, NextElectionTime: nextElectionTime.Unix(), EnableValidation: isEnable}, nil
	return api.ValidationInfo{}, nil
}

// NodeExit node want to exit titan
func (s *Scheduler) NodeExit(ctx context.Context, deviceID string) error {
	s.nodeManager.NodeExited(deviceID)

	return nil
}

func incrDeviceReward(deviceID string, incrReward int64) error {
	return cache.GetDB().UpdateDeviceInfo(deviceID, func(deviceInfo *api.DevicesInfo) {
		deviceInfo.CumulativeProfit += float64(incrReward)
	})
}

// func incrDeviceBlock(deviceID string, blocks int) error {
// 	return cache.GetDB().UpdateDeviceInfo(deviceID, func(deviceInfo *api.DevicesInfo) {
// 		deviceInfo.BlockCount += blocks
// 	})
// }

func (s *Scheduler) nodeOfflineCallback(deviceID string) {
	s.locatorManager.NotifyNodeStatusToLocator(deviceID, false)
}

func (s *Scheduler) nodeExitedCallback(deviceID string) {
	// clean node cache
	s.dataManager.CleanNodeAndRestoreCaches(deviceID)
}

func (s *Scheduler) authNew() ([]byte, error) {
	tk, err := s.AuthNew(context.Background(), []auth.Permission{api.PermRead, api.PermWrite})
	if err != nil {
		log.Errorf("findDownloadinfoForBlocks AuthNew err:%s", err.Error())
		return nil, err
	}

	return tk, nil
}
