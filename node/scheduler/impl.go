package scheduler

import (
	"context"
	"crypto/rsa"
	"encoding/hex"
	"fmt"
	"math/rand"
	"net/http"
	"time"

	// "github.com/linguohua/titan/node/device"

	"github.com/google/uuid"
	logging "github.com/ipfs/go-log/v2"
	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/api/client"
	"github.com/linguohua/titan/node/common"
	"github.com/linguohua/titan/node/handler"
	"github.com/linguohua/titan/node/helper"

	// "github.com/linguohua/titan/node/device"
	"github.com/linguohua/titan/node/repo"
	"github.com/linguohua/titan/node/scheduler/db/cache"
	"github.com/linguohua/titan/node/scheduler/db/persistent"
	"github.com/linguohua/titan/node/scheduler/web"
	"github.com/linguohua/titan/node/secret"
	"golang.org/x/xerrors"
)

var (
	log    = logging.Logger("scheduler")
	myRand = rand.New(rand.NewSource(time.Now().UnixNano()))
)

const (
	// ErrNodeNotFind node not found
	ErrNodeNotFind = "Not Found Node"
	// ErrCidNotFind node not found
	ErrCidNotFind = "Not Found Cid"
	// ErrUnknownNodeType unknown node type
	ErrUnknownNodeType = "Unknown Node Type"
	// ErrAreaNotExist Area not exist
	ErrAreaNotExist = "%s, Area not exist! ip:%s"
	// ErrNotFoundTask Not Found Task
	ErrNotFoundTask = "Not Found Data Task"
	// ErrCidIsNil node not found
	ErrCidIsNil = "Cid Is Nil"
	// ErrCacheIDIsNil CacheID Is Nil
	ErrCacheIDIsNil = "CacheID Is Nil"
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
func NewLocalScheduleNode(lr repo.LockedRepo, port int) api.Scheduler {
	verifiedNodeMax := 10

	locatorManager := newLoactorManager(port)
	pool := newValidatePool(verifiedNodeMax)
	manager := newNodeManager(pool, locatorManager)
	election := newElection(pool)
	validate := newValidate(pool, manager)
	dataManager := newDataManager(manager)

	s := &Scheduler{
		CommonAPI:      common.NewCommonAPI(manager.updateLastRequestTime),
		nodeManager:    manager,
		validatePool:   pool,
		election:       election,
		validate:       validate,
		dataManager:    dataManager,
		locatorManager: locatorManager,
		serverPort:     port,
	}

	s.Web = web.NewWeb(s)

	sec, err := secret.APISecret(lr)
	if err != nil {
		log.Panicf("NewLocalScheduleNode failed:%s", err.Error())
	}
	s.APISecret = sec

	return s
}

// Scheduler node
type Scheduler struct {
	common.CommonAPI
	api.Web

	nodeManager    *NodeManager
	validatePool   *ValidatePool
	election       *Election
	validate       *Validate
	dataManager    *DataManager
	locatorManager *LocatorManager
	selector       *ValidateSelector

	serverPort int
}

// CandidateNodeConnect Candidate connect
func (s *Scheduler) CandidateNodeConnect(ctx context.Context, rpcURL, downloadSrvURL string) error {
	ip := handler.GetRequestIP(ctx)
	deviceID := handler.GetDeviceID(ctx)

	if !s.nodeManager.isDeviceExist(deviceID, int(api.NodeCandidate)) {
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
		privateKey, err = pem2PrivateKey(authInfo.PrivateKey)
		if err != nil {
			return err
		}
	} else {
		key, err := generatePrivateKey(1024)
		if err != nil {
			return err
		}
		privateKey = key
	}

	deviceInfo.NodeType = api.NodeCandidate
	deviceInfo.ExternalIp = ip

	candidateNode := &CandidateNode{
		nodeAPI: candicateAPI,
		closer:  closer,

		Node: Node{
			scheduler:      s,
			addr:           rpcURL,
			deviceInfo:     deviceInfo,
			downloadSrvURL: downloadSrvURL,
			privateKey:     privateKey,
		},
	}

	err = s.nodeManager.candidateOnline(candidateNode)
	if err != nil {
		log.Errorf("CandidateNodeConnect addEdgeNode err:%s,deviceID:%s", err.Error(), deviceID)
		return err
	}

	deviceInfo.IpLocation = candidateNode.geoInfo.Geo
	err = s.nodeManager.setDeviceInfo(deviceID, deviceInfo)
	if err != nil {
		log.Errorf("CandidateNodeConnect setDeviceInfo err:%s,deviceID:%s", err.Error(), deviceID)
		return err
	}

	s.locatorManager.notifyNodeStatusToLocator(deviceID, true)

	go doDataSync(candicateAPI, deviceID)

	return nil
}

// EdgeNodeConnect edge connect
func (s *Scheduler) EdgeNodeConnect(ctx context.Context, rpcURL, downloadSrvURL string) error {
	ip := handler.GetRequestIP(ctx)
	deviceID := handler.GetDeviceID(ctx)

	if !s.nodeManager.isDeviceExist(deviceID, int(api.NodeEdge)) {
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
		privateKey, err = pem2PrivateKey(authInfo.PrivateKey)
		if err != nil {
			return err
		}
	} else {
		key, err := generatePrivateKey(1024)
		if err != nil {
			return err
		}
		privateKey = key
	}

	deviceInfo.NodeType = api.NodeEdge
	deviceInfo.ExternalIp = ip

	edgeNode := &EdgeNode{
		nodeAPI: edgeAPI,
		closer:  closer,

		Node: Node{
			scheduler:      s,
			addr:           rpcURL,
			deviceInfo:     deviceInfo,
			downloadSrvURL: downloadSrvURL,
			privateKey:     privateKey,
		},
	}

	err = s.nodeManager.edgeOnline(edgeNode)
	if err != nil {
		log.Errorf("EdgeNodeConnect addEdgeNode err:%s,deviceID:%s", err.Error(), deviceInfo.DeviceId)
		return err
	}

	deviceInfo.IpLocation = edgeNode.geoInfo.Geo
	err = s.nodeManager.setDeviceInfo(deviceID, deviceInfo)
	if err != nil {
		log.Errorf("EdgeNodeConnect set device info: %s", err.Error())
		return err
	}

	// notify locator
	s.locatorManager.notifyNodeStatusToLocator(deviceID, true)

	go doDataSync(edgeAPI, deviceID)

	return nil
}

// GetPublicKey get node Public Key
func (s *Scheduler) GetPublicKey(ctx context.Context) (string, error) {
	deviceID := handler.GetDeviceID(ctx)

	edgeNode := s.nodeManager.getEdgeNode(deviceID)
	if edgeNode != nil {
		return publicKey2Pem(&edgeNode.privateKey.PublicKey), nil
	}

	candidateNode := s.nodeManager.getCandidateNode(deviceID)
	if candidateNode != nil {
		return publicKey2Pem(&candidateNode.privateKey.PublicKey), nil
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

	if !s.nodeManager.isDeviceExist(deviceID, 0) {
		return xerrors.Errorf("node not Exist: %s", deviceID)
	}

	vs := &validateResults
	vs.DeviceID = deviceID

	err := s.validate.pushResultToQueue(vs)
	if err != nil {
		log.Errorf("ValidateBlockResult err:%s", err.Error())
	}

	return err
}

// NodeDownloadBlockResult node result for user download block
func (s *Scheduler) NodeDownloadBlockResult(ctx context.Context, result api.NodeBlockDownloadResult) error {
	deviceID := handler.GetDeviceID(ctx)

	if !s.nodeManager.isDeviceExist(deviceID, 0) {
		return xerrors.Errorf("node not Exist: %s", deviceID)
	}

	record, err := cache.GetDB().GetDownloadBlockRecord(result.SN)
	if err != nil {
		log.Errorf("NodeDownloadBlockResult, GetBlockDownloadRecord error:%s", err.Error())
		return err
	}

	err = s.verifyNodeDownloadBlockSign(deviceID, record, result.Sign)
	if err != nil {
		log.Errorf("NodeDownloadBlockResult, verifyNodeDownloadBlockSign error:%s", err.Error())
		return err
	}

	record.NodeStatus = int(blockDownloadStatusFailed)
	if result.Result {
		record.NodeStatus = int(blockDownloadStatusSuccess)
	}

	reward := int64(0)
	if record.NodeStatus == int(blockDownloadStatusSuccess) && record.UserStatus == int(blockDownloadStatusSuccess) {
		reward = 1
		// add reward
		if err := incrDeviceReward(deviceID, reward); err != nil {
			return err
		}
	}

	s.recordDownloadBlock(record, &result, int(reward), deviceID)
	return nil
}

func (s *Scheduler) handleUserDownloadBlockResult(ctx context.Context, result api.UserBlockDownloadResult) error {
	record, err := cache.GetDB().GetDownloadBlockRecord(result.SN)
	if err != nil {
		log.Errorf("handleUserDownloadBlockResult, GetBlockDownloadRecord error:%s", err.Error())
		return err
	}

	err = s.verifyUserDownloadBlockSign(record.UserPublicKey, record.Cid, result.Sign)
	if err != nil {
		log.Errorf("handleUserDownloadBlockResult, verifyNodeDownloadBlockSign error:%s", err.Error())
		return err
	}

	record.UserStatus = int(blockDownloadStatusFailed)
	if result.Result {
		record.UserStatus = int(blockDownloadStatusSuccess)
	}

	var deviceID string

	reward := int64(0)
	if record.NodeStatus == int(blockDownloadStatusSuccess) && record.UserStatus == int(blockDownloadStatusSuccess) {
		reward = 1
		// add reward
		if err := incrDeviceReward(deviceID, reward); err != nil {
			return err
		}
	}

	s.recordDownloadBlock(record, nil, 0, "")
	return nil
}

// UserDownloadBlockResults node result for user download block
func (s *Scheduler) UserDownloadBlockResults(ctx context.Context, results []api.UserBlockDownloadResult) error {
	for _, result := range results {
		err := s.handleUserDownloadBlockResult(ctx, result)
		if err != nil {
			return err
		}
	}
	return nil
}

// CacheContinue Cache Continue
func (s *Scheduler) CacheContinue(ctx context.Context, cid, cacheID string) error {
	if cid == "" || cacheID == "" {
		return xerrors.New("parameter is nil")
	}

	return s.dataManager.cacheContinue(cid, cacheID)
}

// CacheResult Cache Data Result
func (s *Scheduler) CacheResult(ctx context.Context, deviceID string, info api.CacheResultInfo) (string, error) {
	deviceID = handler.GetDeviceID(ctx)

	if !s.nodeManager.isDeviceExist(deviceID, 0) {
		return "", xerrors.Errorf("node not Exist: %s", deviceID)
	}

	// log.Warnf("CacheResult ,CacheID:%s Cid:%s", info.CacheID, info.Cid)
	err := s.dataManager.pushCacheResultToQueue(deviceID, &info)

	return "", err
}

// RegisterNode Register Node
func (s *Scheduler) RegisterNode(ctx context.Context, nodeType api.NodeType) (api.NodeRegisterInfo, error) {
	return registerNode(nodeType)
}

// // GetToken get token
// func (s *Scheduler) GetToken(ctx context.Context, deviceID, secret string) (string, error) {
// 	return generateToken(deviceID, secret)
// }

// DeleteBlockRecords  Delete Block Record
func (s *Scheduler) DeleteBlockRecords(ctx context.Context, deviceID string, cids []string) (map[string]string, error) {
	if len(cids) <= 0 {
		return nil, xerrors.New("cids is nil")
	}

	// edge := s.nodeManager.getEdgeNode(deviceID)
	// if edge != nil {
	// 	return edge.deleteBlockRecords(cids)
	// }

	// candidate := s.nodeManager.getCandidateNode(deviceID)
	// if candidate != nil {
	// 	return candidate.deleteBlockRecords(cids)
	// }

	return nil, xerrors.Errorf("%s:%s", ErrNodeNotFind, deviceID)
}

// RemoveCarfile remove all caches with carfile
func (s *Scheduler) RemoveCarfile(ctx context.Context, carfileID string) error {
	if carfileID == "" {
		return xerrors.Errorf(ErrCidIsNil)
	}

	return s.dataManager.removeCarfile(carfileID)
}

// RemoveCache remove a caches with carfile
func (s *Scheduler) RemoveCache(ctx context.Context, carfileID, cacheID string) error {
	if carfileID == "" {
		return xerrors.Errorf(ErrCidIsNil)
	}

	if cacheID == "" {
		return xerrors.Errorf(ErrCacheIDIsNil)
	}

	return s.dataManager.removeCache(carfileID, cacheID)
}

// CacheCarfile Cache Carfile
func (s *Scheduler) CacheCarfile(ctx context.Context, cid string, reliability int, hour int) error {
	if cid == "" {
		return xerrors.New("cid is nil")
	}

	expiredTime := time.Now().Add(time.Duration(hour) * time.Hour)

	return s.dataManager.cacheData(cid, reliability, expiredTime)
}

// ListDatas List Datas
func (s *Scheduler) ListDatas(ctx context.Context, page int) (api.DataListInfo, error) {
	count, totalPage, list, err := persistent.GetDB().GetDataCidWithPage(page)
	if err != nil {
		return api.DataListInfo{}, err
	}

	return api.DataListInfo{Page: page, TotalPage: totalPage, Cids: count, CidList: list}, nil
}

// ShowDataTask Show Data Task
func (s *Scheduler) ShowDataTask(ctx context.Context, cid string) (api.CacheDataInfo, error) {
	info := api.CacheDataInfo{}

	if cid == "" {
		return info, xerrors.Errorf("%s:%s", ErrCidNotFind, cid)
	}

	hash, err := helper.CIDString2HashString(cid)
	if err != nil {
		return info, err
	}

	d := s.dataManager.getData(hash)
	if d != nil {
		cInfo := dataToCacheDataInfo(d)
		t, err := cache.GetDB().GetRunningDataTaskExpiredTime(hash)
		if err == nil {
			cInfo.DataTimeout = t
		}

		return cInfo, nil
	}

	return info, xerrors.Errorf("%s:%s", ErrCidNotFind, cid)
}

// GetOnlineDeviceIDs Get all online node id
func (s *Scheduler) GetOnlineDeviceIDs(ctx context.Context, nodeType api.NodeTypeName) ([]string, error) {
	list := make([]string, 0)

	if nodeType == api.TypeNameValidator {
		return s.validatePool.veriftorList, nil
	}

	if nodeType == api.TypeNameAll || nodeType == api.TypeNameCandidate {
		s.nodeManager.candidateNodeMap.Range(func(key, value interface{}) bool {
			deviceID := key.(string)
			list = append(list, deviceID)

			return true
		})
	}

	if nodeType == api.TypeNameAll || nodeType == api.TypeNameEdge {
		s.nodeManager.edgeNodeMap.Range(func(key, value interface{}) bool {
			deviceID := key.(string)
			list = append(list, deviceID)

			return true
		})
	}

	return list, nil
}

// ListEvents get data events
func (s *Scheduler) ListEvents(ctx context.Context, page int) (api.EventListInfo, error) {
	count, totalPage, list, err := persistent.GetDB().GetEventInfos(page)
	if err != nil {
		return api.EventListInfo{}, err
	}

	return api.EventListInfo{Page: page, TotalPage: totalPage, Count: count, EventList: list}, nil
}

// GetCandidateDownloadInfoWithBlocks find node
func (s *Scheduler) GetCandidateDownloadInfoWithBlocks(ctx context.Context, cids []string) (map[string]api.DownloadInfoResult, error) {
	deviceID := handler.GetDeviceID(ctx)

	if !s.nodeManager.isDeviceExist(deviceID, 0) {
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

		infos, err := s.nodeManager.getCandidateNodesWithData(hash, deviceID)
		if err != nil || len(infos) <= 0 {
			continue
		}

		candidate := infos[randomNum(0, len(infos))]

		info, err := persistent.GetDB().GetNodeAuthInfo(candidate.deviceInfo.DeviceId)
		if err != nil {
			continue
		}

		// TODO: complete downloadInfo

		infoMap[cid] = api.DownloadInfoResult{URL: info.URL}
	}

	return infoMap, nil
}

// GetDownloadInfosWithBlocks find node
func (s *Scheduler) GetDownloadInfosWithBlocks(ctx context.Context, cids []string, publicKey string) (map[string][]api.DownloadInfoResult, error) {
	if len(cids) < 1 {
		return nil, xerrors.New("cids is nil")
	}

	devicePrivateKey := make(map[string]*rsa.PrivateKey)
	infoMap := make(map[string][]api.DownloadInfoResult)

	for _, cid := range cids {
		infos, err := s.nodeManager.findNodeDownloadInfos(cid)
		if err != nil {
			continue
		}

		sn, err := cache.GetDB().IncrBlockDownloadSN()
		if err != nil {
			continue
		}

		signTime := time.Now().Unix()
		infos, err = s.signDownloadInfos(cid, sn, signTime, infos, devicePrivateKey)
		if err != nil {
			continue
		}
		infoMap[cid] = infos

		record := cache.DownloadBlockRecord{
			SN:            sn,
			ID:            uuid.New().String(),
			Cid:           cid,
			SignTime:      signTime,
			Timeout:       blockDonwloadTimeout,
			UserPublicKey: publicKey,
			NodeStatus:    int(blockDownloadStatusUnknow),
			UserStatus:    int(blockDownloadStatusUnknow),
		}
		err = s.recordDownloadBlock(record, nil, 0, "")
		if err != nil {
			log.Errorf("GetDownloadInfosWithBlocks,recordDownloadBlock error %s", err.Error())
		}
	}

	return infoMap, nil
}

// GetDownloadInfoWithBlocks find node
func (s *Scheduler) GetDownloadInfoWithBlocks(ctx context.Context, cids []string, publicKey string) (map[string]api.DownloadInfoResult, error) {
	if len(cids) < 1 {
		return nil, xerrors.New("cids is nil")
	}

	devicePrivateKey := make(map[string]*rsa.PrivateKey)
	infoMap := make(map[string]api.DownloadInfoResult)

	for _, cid := range cids {
		infos, err := s.nodeManager.findNodeDownloadInfos(cid)
		if err != nil {
			continue
		}

		info := infos[randomNum(0, len(infos))]

		sn, err := cache.GetDB().IncrBlockDownloadSN()
		if err != nil {
			continue
		}

		signTime := time.Now().Unix()
		infos, err = s.signDownloadInfos(cid, sn, signTime, []api.DownloadInfoResult{info}, devicePrivateKey)
		if err != nil {
			continue
		}

		infoMap[cid] = infos[0]

		record := cache.DownloadBlockRecord{
			SN:            sn,
			ID:            uuid.New().String(),
			Cid:           cid,
			SignTime:      signTime,
			Timeout:       blockDonwloadTimeout,
			UserPublicKey: publicKey,
			NodeStatus:    int(blockDownloadStatusUnknow),
			UserStatus:    int(blockDownloadStatusUnknow),
		}
		err = s.recordDownloadBlock(record, nil, 0, "")
		if err != nil {
			log.Errorf("GetDownloadInfoWithBlocks,recordDownloadBlock error %s", err.Error())
		}
	}

	return infoMap, nil
}

// GetDownloadInfoWithBlock find node
func (s *Scheduler) GetDownloadInfoWithBlock(ctx context.Context, cid string, publicKey string) (api.DownloadInfoResult, error) {
	if cid == "" {
		return api.DownloadInfoResult{}, xerrors.New("cids is nil")
	}

	infos, err := s.nodeManager.findNodeDownloadInfos(cid)
	if err != nil {
		return api.DownloadInfoResult{}, err
	}

	info := infos[randomNum(0, len(infos))]

	sn, err := cache.GetDB().IncrBlockDownloadSN()
	if err != nil {
		return api.DownloadInfoResult{}, err
	}
	signTime := time.Now().Unix()
	infos, err = s.signDownloadInfos(cid, sn, signTime, []api.DownloadInfoResult{info}, make(map[string]*rsa.PrivateKey))
	if err != nil {
		return api.DownloadInfoResult{}, err
	}

	record := cache.DownloadBlockRecord{
		SN:            sn,
		ID:            uuid.New().String(),
		Cid:           cid,
		SignTime:      signTime,
		Timeout:       blockDonwloadTimeout,
		UserPublicKey: publicKey,
		NodeStatus:    int(blockDownloadStatusUnknow),
		UserStatus:    int(blockDownloadStatusUnknow),
	}

	err = s.recordDownloadBlock(record, nil, 0, "")
	if err != nil {
		log.Errorf("GetDownloadInfoWithBlock,recordDownloadBlock error %s", err.Error())
	}

	return infos[0], nil
}

// QueryCacheStatWithNode Query Cache Stat
func (s *Scheduler) QueryCacheStatWithNode(ctx context.Context, deviceID string) ([]api.CacheStat, error) {
	statList := make([]api.CacheStat, 0)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// node datas
	candidata := s.nodeManager.getCandidateNode(deviceID)
	if candidata != nil {
		// redis datas
		body := api.CacheStat{}
		count, err := persistent.GetDB().GetDeviceBlockNum(deviceID)
		if err == nil {
			body.CacheBlockCount = int(count)
		}

		statList = append(statList, body)

		nodeBody, _ := candidata.nodeAPI.QueryCacheStat(ctx)
		statList = append(statList, nodeBody)
		return statList, nil
	}

	edge := s.nodeManager.getEdgeNode(deviceID)
	if edge != nil {
		body := api.CacheStat{}
		count, err := persistent.GetDB().GetDeviceBlockNum(deviceID)
		if err == nil {
			body.CacheBlockCount = int(count)
		}

		statList = append(statList, body)

		nodeBody, _ := edge.nodeAPI.QueryCacheStat(ctx)
		statList = append(statList, nodeBody)
		return statList, nil
	}

	return statList, xerrors.Errorf("%s:%s", ErrNodeNotFind, deviceID)
}

// QueryCachingBlocksWithNode Query Caching Blocks
func (s *Scheduler) QueryCachingBlocksWithNode(ctx context.Context, deviceID string) (api.CachingBlockList, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	candidata := s.nodeManager.getCandidateNode(deviceID)
	if candidata != nil {
		return candidata.nodeAPI.QueryCachingBlocks(ctx)
	}

	edge := s.nodeManager.getEdgeNode(deviceID)
	if edge != nil {
		return edge.nodeAPI.QueryCachingBlocks(ctx)
	}

	return api.CachingBlockList{}, xerrors.Errorf("%s:%s", ErrNodeNotFind, deviceID)
}

// ElectionValidators Election Validators
func (s *Scheduler) ElectionValidators(ctx context.Context) error {
	return s.election.startElection()
}

// Validate Validate edge
func (s *Scheduler) Validate(ctx context.Context) error {
	return s.validate.startValidate()
}

// GetDevicesInfo return the devices information
func (s *Scheduler) GetDevicesInfo(ctx context.Context, deviceID string) (api.DevicesInfo, error) {
	// node datas
	deviceInfo, err := cache.GetDB().GetDeviceInfo(deviceID)
	if err != nil {
		log.Errorf("getNodeInfo: %s ,deviceID : %s", err.Error(), deviceID)
		return api.DevicesInfo{}, err
	}

	_, isOnline := s.nodeManager.candidateNodeMap.Load(deviceID)
	if !isOnline {
		_, isOnline = s.nodeManager.edgeNodeMap.Load(deviceID)
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
	s.validate.open = open
	return nil
}

// LocatorConnect Locator Connect
func (s *Scheduler) LocatorConnect(ctx context.Context, port int, areaID, locatorID string, locatorToken string) error {
	ip := handler.GetRequestIP(ctx)
	url := fmt.Sprintf("http://%s:%d/rpc/v0", ip, port)
	log.Infof("LocatorConnect locatorID:%s,areaID:%s,LocatorConnect ip:%s,port:%d", locatorID, areaID, ip, port)

	if areaID != serverArea {
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

	s.locatorManager.addLocator(&Location{locatorID: locatorID, nodeAPI: locationAPI, closer: closer})

	return nil
}

// GetDownloadInfo get node download info
func (s *Scheduler) GetDownloadInfo(ctx context.Context, deviceID string) ([]*api.BlockDownloadInfo, error) {
	return persistent.GetDB().GetBlockDownloadInfoByDeviceID(deviceID)
}

// ShowDataTasks Show Data Tasks
func (s *Scheduler) ShowDataTasks(ctx context.Context) ([]api.CacheDataInfo, error) {
	infos := make([]api.CacheDataInfo, 0)

	list := s.dataManager.getRunningTasks()

	for _, info := range list {
		data := s.dataManager.getData(info.CarfileHash)
		if data != nil {
			cInfo := dataToCacheDataInfo(data)

			t, err := cache.GetDB().GetRunningDataTaskExpiredTime(info.CarfileHash)
			if err == nil {
				cInfo.DataTimeout = t
			}

			infos = append(infos, cInfo)
		}
	}

	// s.dataManager.taskMap.Range(func(key, value interface{}) bool {
	// 	data := value.(*Data)

	// 	infos = append(infos, dataToCacheDataInfo(data))

	// 	return true
	// })

	// log.Infof("ShowDataTasks:%v", infos)
	return infos, nil
}

func dataToCacheDataInfo(d *Data) api.CacheDataInfo {
	info := api.CacheDataInfo{}
	if d != nil {
		info.CarfileCid = d.carfileCid
		info.CarfileHash = d.carfileHash
		info.TotalSize = d.totalSize
		info.NeedReliability = d.needReliability
		info.CurReliability = d.reliability
		info.Blocks = d.totalBlocks
		info.Nodes = d.nodes

		caches := make([]api.CacheInfo, 0)

		d.cacheMap.Range(func(key, value interface{}) bool {
			c := value.(*Cache)

			cache := api.CacheInfo{
				CacheID:    c.cacheID,
				Status:     int(c.status),
				DoneSize:   c.doneSize,
				DoneBlocks: c.doneBlocks,
				Nodes:      c.nodes,
			}

			caches = append(caches, cache)
			return true
		})

		info.CacheInfos = caches
	}

	return info
}

// UpdateDownloadServerAccessAuth Update Access Auth
// func (s *Scheduler) UpdateDownloadServerAccessAuth(ctx context.Context, access api.DownloadServerAccessAuth) error {
// 	deviceID := handler.GetDeviceID(ctx)

// 	if !s.nodeManager.isDeviceExist(deviceID, 0) {
// 		return xerrors.Errorf("node not Exist: %s", deviceID)
// 	}

// 	info := &access
// 	info.DeviceID = deviceID

// 	cNode := s.nodeManager.getCandidateNode(info.DeviceID)
// 	if cNode != nil {
// 		return cNode.updateAccessAuth(info)
// 	}

// 	eNode := s.nodeManager.getEdgeNode(info.DeviceID)
// 	if eNode != nil {
// 		return eNode.updateAccessAuth(info)
// 	}

// 	return xerrors.Errorf("%s :%s", ErrNodeNotFind, info.DeviceID)
// }

// GetValidationInfo Get Validation Info
func (s *Scheduler) GetValidationInfo(ctx context.Context) (api.ValidationInfo, error) {
	nextElectionTime := s.selector.getNextElectionTime()
	isEnable := s.validate.open

	validators, err := cache.GetDB().GetValidatorsWithList()
	if err != nil {
		return api.ValidationInfo{}, err
	}

	return api.ValidationInfo{Validators: validators, NextElectionTime: nextElectionTime.Unix(), EnableValidation: isEnable}, nil
}

// func (s *Scheduler) checkToBeDeleteBlocks(deviceID string) error {
// 	dBlocks, err := persistent.GetDB().GetToBeDeleteBlocks(deviceID)
// 	if err != nil {
// 		log.Errorf("checkToBeDeleteBlocks GetToBeDeleteBlocks err:%s,deviceID:%s", err.Error(), deviceID)
// 		return err
// 	}

// 	list := make([]string, 0)
// 	blockDeletes := make([]*persistent.BlockDelete, 0)
// 	// check block is in new cache
// 	for _, info := range dBlocks {
// 		nBlock, err := persistent.GetDB().GetNodeBlock(deviceID, info.CID)
// 		if err != nil {
// 			continue
// 		}

// 		blockDeletes = append(blockDeletes, &persistent.BlockDelete{CID: info.CID, DeviceID: deviceID})

// 		if nBlock != nil && nBlock.CacheID != info.CacheID {
// 			// in new cache
// 			continue
// 		}

// 		list = append(list, info.CID)
// 	}

// 	ctx := context.Background()
// 	candidata := s.nodeManager.getCandidateNode(deviceID)
// 	if candidata != nil {
// 		_, err = candidata.nodeAPI.DeleteBlocks(ctx, list)
// 		if err != nil {
// 			return err
// 		}

// 		return persistent.GetDB().RemoveToBeDeleteBlock(blockDeletes)
// 	}

// 	edge := s.nodeManager.getEdgeNode(deviceID)
// 	if edge != nil {
// 		_, err = edge.nodeAPI.DeleteBlocks(ctx, list)
// 		if err != nil {
// 			return err
// 		}

// 		return persistent.GetDB().RemoveToBeDeleteBlock(blockDeletes)
// 	}

// 	return xerrors.New(ErrNodeNotFind)
// }

func (s *Scheduler) verifyNodeDownloadBlockSign(deviceID string, record cache.DownloadBlockRecord, sign []byte) error {
	verifyContent := fmt.Sprintf("%s%d%d%d", record.Cid, record.SN, record.SignTime, record.Timeout)
	edgeNode := s.nodeManager.getEdgeNode(deviceID)
	if edgeNode != nil {
		return verifyRsaSign(&edgeNode.privateKey.PublicKey, sign, verifyContent)
	}

	candidate := s.nodeManager.getCandidateNode(deviceID)
	if candidate != nil {
		return verifyRsaSign(&candidate.privateKey.PublicKey, sign, verifyContent)
	}

	authInfo, err := persistent.GetDB().GetNodeAuthInfo(deviceID)
	if err != nil {
		return err
	}

	privateKey, err := pem2PrivateKey(authInfo.PrivateKey)
	if err != nil {
		return err
	}
	return verifyRsaSign(&privateKey.PublicKey, sign, verifyContent)
}

func (s *Scheduler) verifyUserDownloadBlockSign(publicPem, cid string, sign []byte) error {
	publicKey, err := pem2PublicKey(publicPem)
	if err != nil {
		return err
	}
	return verifyRsaSign(publicKey, sign, cid)
}

func (s *Scheduler) signDownloadInfos(cid string, sn int64, signTime int64, results []api.DownloadInfoResult, devicePrivateKeys map[string]*rsa.PrivateKey) ([]api.DownloadInfoResult, error) {
	downloadInfoResults := make([]api.DownloadInfoResult, 0, len(results))
	for _, result := range results {
		privateKey, ok := devicePrivateKeys[result.DeviceID]
		if !ok {
			var err error
			privateKey, err = s.getDeviccePrivateKey(result.DeviceID)
			if err != nil {
				return nil, err
			}
			devicePrivateKeys[result.DeviceID] = privateKey
		}

		sign, err := rsaSign(privateKey, fmt.Sprintf("%s%d%d%d", cid, sn, signTime, blockDonwloadTimeout))
		if err != nil {
			return nil, err
		}

		downloadInfoResult := api.DownloadInfoResult{URL: result.URL, Sign: hex.EncodeToString(sign), SN: sn, SignTime: signTime, TimeOut: blockDonwloadTimeout}
		downloadInfoResults = append(downloadInfoResults, downloadInfoResult)
	}

	return downloadInfoResults, nil
}

func (s *Scheduler) getDeviccePrivateKey(deviceID string) (*rsa.PrivateKey, error) {
	edge := s.nodeManager.getEdgeNode(deviceID)
	if edge != nil {
		return edge.privateKey, nil
	}

	candidate := s.nodeManager.getCandidateNode(deviceID)
	if candidate != nil {
		return candidate.privateKey, nil
	}

	authInfo, err := persistent.GetDB().GetNodeAuthInfo(deviceID)
	if err != nil {
		return nil, err
	}

	privateKey, err := pem2PrivateKey(authInfo.PrivateKey)
	if err != nil {
		return nil, err
	}

	return privateKey, nil
}

func (s *Scheduler) recordDownloadBlock(record cache.DownloadBlockRecord, nodeResult *api.NodeBlockDownloadResult, reward int, deviceID string) error {
	info, err := persistent.GetDB().GetBlockDownloadInfoByID(record.ID)
	if err != nil {
		return err
	}

	if info == nil {
		info = &api.BlockDownloadInfo{ID: record.ID, BlockCID: record.Cid, CreatedTime: time.Unix(record.SignTime, 0)}
	}

	if nodeResult != nil {
		info.Speed = int64(nodeResult.DownloadSpeed)
		info.BlockSize = nodeResult.BlockSize
		info.ClientIP = nodeResult.ClientIP
		info.FailedReason = nodeResult.FailedReason
	}

	if len(deviceID) > 0 {
		info.DeviceID = deviceID
	}

	if reward > 0 {
		info.Reward = int64(reward)
	}

	if record.NodeStatus == int(blockDownloadStatusFailed) || record.UserStatus == int(blockDownloadStatusFailed) {
		info.Status = int(blockDownloadStatusFailed)
	}

	if record.NodeStatus == int(blockDownloadStatusSuccess) && record.UserStatus == int(blockDownloadStatusSuccess) {
		info.CompleteTime = time.Now()
		info.Status = int(blockDownloadStatusSuccess)
		err = cache.GetDB().RemoveDownloadBlockRecord(record.SN)
	} else {
		err = cache.GetDB().SetDownloadBlockRecord(record)
	}

	if err != nil {
		return err
	}

	return persistent.GetDB().SetBlockDownloadInfo(info)
}

// ResetCacheExpiredTime reset expired time with data cache
func (s *Scheduler) ResetCacheExpiredTime(ctx context.Context, carfileCid, cacheID string, expiredTime time.Time) error {
	return s.dataManager.resetExpiredTime(carfileCid, cacheID, expiredTime)
}

// ReplenishCacheExpiredTime replenish expired time with data cache
func (s *Scheduler) ReplenishCacheExpiredTime(ctx context.Context, carfileCid, cacheID string, hour int) error {
	if hour <= 0 {
		return xerrors.Errorf("hour is :%d", hour)
	}

	return s.dataManager.replenishExpiredTimeToData(carfileCid, cacheID, hour)
}

// NodeExits node want to exits titan
func (s *Scheduler) NodeExits(ctx context.Context, deviceID string) error {
	s.dataManager.cleanNodeAndRestoreCaches(deviceID)

	// TODO remove node manager nodemap and db

	return nil
}

func updateLatency(deviceID string, latency float64) error {
	return cache.GetDB().UpdateDeviceInfo(deviceID, func(deviceInfo *api.DevicesInfo) {
		deviceInfo.Latency = latency
	})
}

func incrDeviceReward(deviceID string, incrReward int64) error {
	return cache.GetDB().UpdateDeviceInfo(deviceID, func(deviceInfo *api.DevicesInfo) {
		lastRewardDate, _ := time.Parse(time.RFC3339, deviceInfo.LastRewardDate)
		if getStartOfDay(lastRewardDate).Equal(getStartOfDay(time.Now())) {
			deviceInfo.TodayProfit += float64(incrReward)
			return
		}
		deviceInfo.TodayProfit = float64(incrReward)
		deviceInfo.LastRewardDate = getStartOfDay(time.Now()).Format(time.RFC3339)
	})
}

func getStartOfDay(t time.Time) time.Time {
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.Local)
}
