package scheduler

import (
	"context"
	"fmt"
	"math/rand"
	"net/http"
	"time"

	// "github.com/linguohua/titan/node/device"

	logging "github.com/ipfs/go-log/v2"
	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/api/client"
	"github.com/linguohua/titan/lib/token"
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
)

type blockDownloadVerifyStatus int

const (
	blockDownloadVerifyUnknow blockDownloadVerifyStatus = iota
	blockDownloadVerifySuccess
	blockDownloadVerifyFailed
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

	go manager.stateNetwork()

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
func (s *Scheduler) CandidateNodeConnect(ctx context.Context, port int) (externalIP string, err error) {
	ip := handler.GetRequestIP(ctx)
	deviceID := handler.GetDeviceID(ctx)

	if !s.nodeManager.isDeviceExist(deviceID, int(api.NodeCandidate)) {
		return "", xerrors.Errorf("candidate node not Exist: %s", deviceID)
	}

	url := fmt.Sprintf("http://%s:%d/rpc/v0", ip, port)
	log.Infof("CandidateNodeConnect %s ;ip:%s,port:%d", deviceID, ip, port)

	t, err := s.AuthNew(ctx, api.AllPermissions)
	if err != nil {
		return "", xerrors.Errorf("creating auth token for remote connection: %s", err.Error())
	}

	headers := http.Header{}
	headers.Add("Authorization", "Bearer "+string(t))

	// Connect to node
	candicateAPI, closer, err := client.NewCandicate(ctx, url, headers)
	if err != nil {
		log.Errorf("CandidateNodeConnect NewCandicate err:%s,url:%s", err.Error(), url)
		return "", err
	}

	// load device info
	deviceInfo, err := candicateAPI.DeviceInfo(ctx)
	if err != nil {
		log.Errorf("CandidateNodeConnect DeviceInfo err:%s", err.Error())
		return "", err
	}

	if deviceID != deviceInfo.DeviceId {
		return "", xerrors.Errorf("deviceID mismatch %s,%s", deviceID, deviceInfo.DeviceId)
	}

	deviceInfo.NodeType = api.NodeCandidate
	deviceInfo.ExternalIp = ip

	candidateNode := &CandidateNode{
		nodeAPI: candicateAPI,
		closer:  closer,

		Node: Node{
			addr:       url,
			deviceInfo: deviceInfo,
		},
	}

	err = s.nodeManager.candidateOnline(candidateNode)
	if err != nil {
		log.Errorf("CandidateNodeConnect addEdgeNode err:%s,deviceID:%s", err.Error(), deviceID)
		return "", err
	}

	deviceInfo.IpLocation = candidateNode.geoInfo.Geo
	err = s.nodeManager.setDeviceInfo(deviceID, deviceInfo)
	if err != nil {
		log.Errorf("CandidateNodeConnect setDeviceInfo err:%s,deviceID:%s", err.Error(), deviceID)
		return "", err
	}

	s.locatorManager.notifyNodeStatusToLocator(deviceID, true)

	go doDataSync(candicateAPI, deviceID)

	return ip, nil
}

// EdgeNodeConnect edge connect
func (s *Scheduler) EdgeNodeConnect(ctx context.Context, port int) (externalIP string, err error) {
	ip := handler.GetRequestIP(ctx)
	deviceID := handler.GetDeviceID(ctx)

	if !s.nodeManager.isDeviceExist(deviceID, int(api.NodeEdge)) {
		return "", xerrors.Errorf("edge node not Exist: %s", deviceID)
	}

	url := fmt.Sprintf("http://%s:%d/rpc/v0", ip, port)
	log.Infof("EdgeNodeConnect %s ;ip:%s,port:%d", deviceID, ip, port)

	t, err := s.AuthNew(ctx, api.AllPermissions)
	if err != nil {
		return "", xerrors.Errorf("creating auth token for remote connection: %s", err.Error())
	}

	headers := http.Header{}
	headers.Add("Authorization", "Bearer "+string(t))

	// Connect to node
	edgeAPI, closer, err := client.NewEdge(ctx, url, headers)
	if err != nil {
		log.Errorf("EdgeNodeConnect NewEdge err:%s,url:%s", err.Error(), url)
		return "", err
	}

	// load device info
	deviceInfo, err := edgeAPI.DeviceInfo(ctx)
	if err != nil {
		log.Errorf("EdgeNodeConnect DeviceInfo err:%s", err.Error())
		return "", err
	}

	if deviceID != deviceInfo.DeviceId {
		return "", xerrors.Errorf("deviceID mismatch %s,%s", deviceID, deviceInfo.DeviceId)
	}

	deviceInfo.NodeType = api.NodeEdge
	deviceInfo.ExternalIp = ip

	edgeNode := &EdgeNode{
		nodeAPI: edgeAPI,
		closer:  closer,

		Node: Node{
			addr:       url,
			deviceInfo: deviceInfo,
		},
	}

	err = s.nodeManager.edgeOnline(edgeNode)
	if err != nil {
		log.Errorf("EdgeNodeConnect addEdgeNode err:%s,deviceID:%s", err.Error(), deviceInfo.DeviceId)
		return "", err
	}

	deviceInfo.IpLocation = edgeNode.geoInfo.Geo
	err = s.nodeManager.setDeviceInfo(deviceID, deviceInfo)
	if err != nil {
		log.Errorf("EdgeNodeConnect set device info: %s", err.Error())
		return "", err
	}

	// notify locator
	s.locatorManager.notifyNodeStatusToLocator(deviceID, true)

	go doDataSync(edgeAPI, deviceID)

	return ip, nil
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

// DownloadBlockResult node result for user download block
func (s *Scheduler) NodeDownloadBlockResult(ctx context.Context, result api.NodeBlockDownloadResult) error {
	deviceID := handler.GetDeviceID(ctx)

	if !s.nodeManager.isDeviceExist(deviceID, 0) {
		return xerrors.Errorf("node not Exist: %s", deviceID)
	}

	ok, err := s.verifyNodeDownloadBlockSign(deviceID, result.Sign)
	if err != nil {
		log.Errorf("NodeDownloadBlockResult, verifyNodeDownloadBlockSign error:%s", err.Error())
		return err
	}

	record, err := cache.GetDB().GetBlockDownloadRecord(result.SN)
	if err != nil {
		log.Errorf("NodeDownloadBlockResult, GetBlockDownloadRecord error:%s", err.Error())
		return err
	}

	record.NodeVerifyStatus = int(blockDownloadVerifyFailed)
	if ok {
		record.NodeVerifyStatus = int(blockDownloadVerifySuccess)
	}

	reward := int64(0)
	if record.NodeVerifyStatus == int(blockDownloadVerifySuccess) && record.UserVerifyStatus == int(blockDownloadVerifySuccess) {
		reward = 1
		// add reward
		if err := cache.GetDB().IncrDeviceReward(deviceID, reward); err != nil {
			return err
		}
	}

	// TODO update downloadInfo
	return persistent.GetDB().AddDownloadInfo(deviceID, &api.BlockDownloadInfo{
		DeviceID: deviceID,
		BlockCID: record.Cid,
		Reward:   reward,
		// BlockSize: int64(stat.BlockSize),
		Speed: result.DownloadSpeed,
	})
}

func (s *Scheduler) handleUserDownloadBlockResult(result api.UserBlockDownloadResult) error {
	record, err := cache.GetDB().GetBlockDownloadRecord(result.SN)
	if err != nil {
		log.Errorf("NodeDownloadBlockResult, GetBlockDownloadRecord error:%s", err.Error())
		return err
	}

	ok, err := s.verifyUserDownloadBlockSign(record.UserPublicKey, result.Sign)
	if err != nil {
		log.Errorf("NodeDownloadBlockResult, verifyNodeDownloadBlockSign error:%s", err.Error())
		return err
	}

	record.NodeVerifyStatus = int(blockDownloadVerifyFailed)
	if ok {
		record.NodeVerifyStatus = int(blockDownloadVerifySuccess)
	}

	var deviceID string

	reward := int64(0)
	if record.NodeVerifyStatus == int(blockDownloadVerifySuccess) && record.UserVerifyStatus == int(blockDownloadVerifySuccess) {
		reward = 1
		// add reward
		if err := cache.GetDB().IncrDeviceReward(deviceID, reward); err != nil {
			return err
		}
	}

	// return persistent.GetDB().AddDownloadInfo(deviceID, &api.BlockDownloadInfo{
	// 	DeviceID:  deviceID,
	// 	BlockCID:  record.Cid,
	// 	Reward:    reward,
	// 	BlockSize: int64(stat.BlockSize),
	// 	Speed:     result.DownloadSpeed,
	// })

	// TODO update downloadInfo

	return nil
}

// DownloadBlockResult node result for user download block
func (s *Scheduler) UserDownloadBlockResults(ctx context.Context, results []api.UserBlockDownloadResult) error {
	for _, result := range results {
		err := s.handleUserDownloadBlockResult(result)
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

	// log.Warnf("CacheResult deviceID:%s ,cid:%s", deviceID, info.Cid)
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
func (s *Scheduler) CacheCarfile(ctx context.Context, cid string, reliability int, expiredTime int) error {
	if cid == "" {
		return xerrors.New("cid is nil")
	}

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

	d := s.dataManager.findData(cid)
	if d != nil {
		return dataToCacheDataInfo(d), nil
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
func (s *Scheduler) GetCandidateDownloadInfoWithBlocks(ctx context.Context, cids []string) (map[string]api.DownloadInfo, error) {
	deviceID := handler.GetDeviceID(ctx)

	if !s.nodeManager.isDeviceExist(deviceID, 0) {
		return nil, xerrors.Errorf("node not Exist: %s", deviceID)
	}

	if len(cids) < 1 {
		return nil, xerrors.New("cids is nil")
	}

	infoMap := make(map[string]api.DownloadInfo)

	for _, cid := range cids {
		infos, err := s.nodeManager.getCandidateNodesWithData(cid, deviceID)
		if err != nil || len(infos) <= 0 {
			continue
		}

		candidate := infos[randomNum(0, len(infos))]

		info, err := persistent.GetDB().GetNodeAuthInfo(candidate.deviceInfo.DeviceId)
		if err != nil {
			continue
		}

		tk, err := token.GenerateToken(info.SecurityKey, time.Now().Add(helper.DownloadTokenExpireAfter).Unix())
		if err != nil {
			continue
		}

		infoMap[cid] = api.DownloadInfo{URL: info.URL, Token: tk}
	}

	return infoMap, nil
}

// GetDownloadInfosWithBlocks find node
func (s *Scheduler) GetDownloadInfosWithBlocks(ctx context.Context, cids []string) (map[string][]api.DownloadInfo, error) {
	if len(cids) < 1 {
		return nil, xerrors.New("cids is nil")
	}

	infoMap := make(map[string][]api.DownloadInfo)

	for _, cid := range cids {
		infos, err := s.nodeManager.findNodeDownloadInfos(cid)
		if err != nil {
			continue
		}

		infoMap[cid] = infos
	}

	return infoMap, nil
}

// GetDownloadInfoWithBlocks find node
func (s *Scheduler) GetDownloadInfoWithBlocks(ctx context.Context, cids []string) (map[string]api.DownloadInfo, error) {
	if len(cids) < 1 {
		return nil, xerrors.New("cids is nil")
	}

	infoMap := make(map[string]api.DownloadInfo)

	for _, cid := range cids {
		infos, err := s.nodeManager.findNodeDownloadInfos(cid)
		if err != nil {
			continue
		}

		info := infos[randomNum(0, len(infos))]

		infoMap[cid] = info
	}

	return infoMap, nil
}

// GetDownloadInfoWithBlock find node
func (s *Scheduler) GetDownloadInfoWithBlock(ctx context.Context, cid string) (api.DownloadInfo, error) {
	if cid == "" {
		return api.DownloadInfo{}, xerrors.New("cids is nil")
	}

	infos, err := s.nodeManager.findNodeDownloadInfos(cid)
	if err != nil {
		return api.DownloadInfo{}, err
	}

	return infos[randomNum(0, len(infos))], nil
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

// StateNetwork State Network
func (s *Scheduler) StateNetwork(ctx context.Context) (api.StateNetwork, error) {
	return s.nodeManager.state, nil
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
	return persistent.GetDB().GetDownloadInfo(deviceID)
}

// ShowDataTasks Show Data Tasks
func (s *Scheduler) ShowDataTasks(ctx context.Context) ([]api.CacheDataInfo, error) {
	infos := make([]api.CacheDataInfo, 0)

	// list, err := cache.GetDB().GetTasksWithRunningList()
	// if err != nil {
	// 	return infos, xerrors.Errorf("GetTasksWithRunningList err:%s", err.Error())
	// }

	// // log.Infof("ShowDataTasks:%v", list)

	// for _, info := range list {
	// 	data := loadData(info.CarfileCid, s.nodeManager, s.dataManager)
	// 	if data != nil {
	// 		infos = append(infos, dataToCacheDataInfo(data))
	// 	}
	// }

	s.dataManager.taskMap.Range(func(key, value interface{}) bool {
		data := value.(*Data)

		infos = append(infos, dataToCacheDataInfo(data))

		return true
	})

	// log.Infof("ShowDataTasks:%v", infos)
	return infos, nil
}

func dataToCacheDataInfo(d *Data) api.CacheDataInfo {
	info := api.CacheDataInfo{}
	if d != nil {
		info.CarfileCid = d.carfileCid
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
func (s *Scheduler) UpdateDownloadServerAccessAuth(ctx context.Context, access api.DownloadServerAccessAuth) error {
	deviceID := handler.GetDeviceID(ctx)

	if !s.nodeManager.isDeviceExist(deviceID, 0) {
		return xerrors.Errorf("node not Exist: %s", deviceID)
	}

	info := &access
	info.DeviceID = deviceID

	cNode := s.nodeManager.getCandidateNode(info.DeviceID)
	if cNode != nil {
		return cNode.updateAccessAuth(info)
	}

	eNode := s.nodeManager.getEdgeNode(info.DeviceID)
	if eNode != nil {
		return eNode.updateAccessAuth(info)
	}

	return xerrors.Errorf("%s :%s", ErrNodeNotFind, info.DeviceID)
}

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

func (s *Scheduler) verifyNodeDownloadBlockSign(deviceID, sign string) (bool, error) {
	return true, nil
}

func (s *Scheduler) verifyUserDownloadBlockSign(publicKey, sign string) (bool, error) {
	return true, nil
}
