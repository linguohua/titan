package scheduler

import (
	"context"
	"fmt"
	"github.com/linguohua/titan/node/device"
	"net/http"
	"time"

	logging "github.com/ipfs/go-log/v2"
	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/api/client"
	"github.com/linguohua/titan/node/common"
	"github.com/linguohua/titan/node/repo"
	"github.com/linguohua/titan/node/scheduler/db/persistent"
	"github.com/linguohua/titan/node/secret"
	"github.com/linguohua/titan/region"
	"golang.org/x/xerrors"
)

var log = logging.Logger("scheduler")

// ErrNodeNotFind node not found
const ErrNodeNotFind = "Not Found"

// ErrUnknownNodeType unknown node type
const ErrUnknownNodeType = "Unknown Node Type"

// NewLocalScheduleNode NewLocalScheduleNode
func NewLocalScheduleNode(lr repo.LockedRepo) api.Scheduler {
	verifiedNodeMax := 10

	pool := newValidatePool(verifiedNodeMax)
	manager := newNodeManager(pool)
	election := newElection(pool)
	validate := newValidate(pool, manager)
	dataManager := newDataManager(manager)

	s := &Scheduler{
		CommonAPI:    common.NewCommonAPI(manager.updateLastRequestTime),
		nodeManager:  manager,
		validatePool: pool,
		election:     election,
		validate:     validate,
		dataManager:  dataManager,
	}

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

	nodeManager  *NodeManager
	validatePool *ValidatePool

	election *Election
	validate *Validate

	dataManager *DataManager
}

// EdgeNodeConnect edge connect
func (s *Scheduler) EdgeNodeConnect(ctx context.Context, url, token string) error {
	// re, oo := peer.FromContext(ctx)

	deviceID, err := verifySecret(token, api.NodeEdge)
	if err != nil {
		log.Errorf("EdgeNodeConnect verifySecret err:%v", err)
		return err
	}

	t, err := s.AuthNew(ctx, api.AllPermissions)
	if err != nil {
		return xerrors.Errorf("creating auth token for remote connection: %w", err)
	}

	headers := http.Header{}
	headers.Add("Authorization", "Bearer "+string(t))
	// Connect to scheduler
	// log.Infof("EdgeNodeConnect edge url:%v", url)
	edgeAPI, closer, err := client.NewEdge(ctx, url, headers)
	if err != nil {
		log.Errorf("EdgeNodeConnect NewEdge err:%v,url:%v", err, url)
		return err
	}

	// load device info
	deviceInfo, err := edgeAPI.DeviceInfo(ctx)
	if err != nil {
		log.Errorf("EdgeNodeConnect DeviceInfo err:%v", err)
		return err
	}

	if deviceID != deviceInfo.DeviceId {
		return xerrors.Errorf("deviceID mismatch %s,%s", deviceID, deviceInfo.DeviceId)
	}

	edgeNode := &EdgeNode{
		nodeAPI: edgeAPI,
		closer:  closer,

		Node: Node{
			addr:       url,
			deviceInfo: deviceInfo,
		},
	}

	// ok, err := cache.GetDB().IsEdgeInDeviceIDList(deviceInfo.DeviceId)
	// if err != nil || !ok {
	// 	log.Errorf("EdgeNodeConnect IsEdgeInDeviceIDList err:%v,deviceID:%s", err, deviceInfo.DeviceId)
	// 	return xerrors.Errorf("deviceID does not exist")
	// }

	err = s.nodeManager.edgeOnline(edgeNode)
	if err != nil {
		log.Errorf("EdgeNodeConnect addEdgeNode err:%v,deviceID:%s", err, deviceInfo.DeviceId)
		return err
	}

	// cids := edgeNode.getCacheFailCids()
	// if cids != nil && len(cids) > 0 {
	// 	reqDatas, _ := edgeNode.getReqCacheDatas(s, cids, true)

	// 	for _, reqData := range reqDatas {
	// 		err := edgeNode.nodeAPI.CacheBlocks(ctx, reqData)
	// 		if err != nil {
	// 			log.Errorf("EdgeNodeConnect CacheData err:%v,url:%v,cids:%v", err.Error(), reqData.CandidateURL, reqData.Cids)
	// 		}
	// 	}
	// }

	return nil
}

// ValidateBlockResult Validate Block Result
func (s Scheduler) ValidateBlockResult(ctx context.Context, validateResults api.ValidateResults) error {
	err := s.validate.validateResult(&validateResults)
	if err != nil {
		log.Errorf("ValidateBlockResult err:%v", err.Error())
	}

	return err
}

// CacheResult Cache Data Result
func (s *Scheduler) CacheResult(ctx context.Context, deviceID string, info api.CacheResultInfo) (string, error) {
	s.dataManager.cacheResult(deviceID, &info)
	// carfileID, cacheID := s.dataManager.cacheResult(deviceID, info)

	edge := s.nodeManager.getEdgeNode(deviceID)
	if edge != nil {
		return edge.cacheBlockResult(&info)
	}

	candidate := s.nodeManager.getCandidateNode(deviceID)
	if candidate != nil {
		return candidate.cacheBlockResult(&info)
	}

	return "", xerrors.New(ErrNodeNotFind)
}

// RegisterNode Register Node
func (s *Scheduler) RegisterNode(ctx context.Context, nodeType api.NodeType) (api.NodeRegisterInfo, error) {
	return registerNode(nodeType)
}

// GetToken get token
func (s *Scheduler) GetToken(ctx context.Context, deviceID, secret string) (string, error) {
	return generateToken(deviceID, secret)
}

// DeleteBlockRecords  Delete Block Record
func (s *Scheduler) DeleteBlockRecords(ctx context.Context, deviceID string, cids []string) (map[string]string, error) {
	if len(cids) <= 0 {
		return nil, xerrors.New("cids is nil")
	}

	edge := s.nodeManager.getEdgeNode(deviceID)
	if edge != nil {
		return edge.deleteBlockRecords(cids)
	}

	candidate := s.nodeManager.getCandidateNode(deviceID)
	if candidate != nil {
		return candidate.deleteBlockRecords(cids)
	}

	return nil, xerrors.New(ErrNodeNotFind)
}

// DeleteBlocks  Delete Blocks
func (s *Scheduler) DeleteBlocks(ctx context.Context, deviceID string, cids []string) (map[string]string, error) {
	if len(cids) <= 0 {
		return nil, xerrors.New("cids is nil")
	}

	errorMap := make(map[string]string)

	nodeFinded := false

	var node Node

	edge := s.nodeManager.getEdgeNode(deviceID)
	if edge != nil {
		results, err := edge.nodeAPI.DeleteBlocks(ctx, cids)
		if err != nil {
			return nil, err
		}

		nodeFinded = true

		if len(results) > 0 {
			for _, data := range results {
				errorMap[data.Cid] = data.ErrMsg
			}
		}

		node = edge.Node
	}

	candidate := s.nodeManager.getCandidateNode(deviceID)
	if candidate != nil {
		resultList, err := candidate.nodeAPI.DeleteBlocks(ctx, cids)
		if err != nil {
			return nil, err
		}

		nodeFinded = true

		if len(resultList) > 0 {
			for _, data := range resultList {
				errorMap[data.Cid] = data.ErrMsg
			}
		}

		node = candidate.Node
	}

	if !nodeFinded {
		return nil, xerrors.New(ErrNodeNotFind)
	}

	delRecordList := make([]string, 0)
	for _, cid := range cids {
		if errorMap[cid] != "" {
			continue
		}

		delRecordList = append(delRecordList, cid)
	}

	eList, err := node.deleteBlockRecords(delRecordList)
	for cid, eSrt := range eList {
		errorMap[cid] = eSrt
	}

	return errorMap, err
}

// CacheCarFile Cache CarFile
func (s *Scheduler) CacheCarFile(ctx context.Context, cid string, reliability int) error {
	if cid == "" {
		return xerrors.New("cid is nil")
	}

	return s.dataManager.cacheData(cid, reliability)
}

// ShowDataInfos Show DataInfos
func (s *Scheduler) ShowDataInfos(ctx context.Context, cid string) (string, error) {
	str := ""
	if cid == "" {
		// for _, d := range s.dataManager.dataMap {
		// 	str = fmt.Sprintf("%scid:%v,totalSize:%v,cache:", str, d.cid, d.totalSize)
		// 	for _, c := range d.cacheMap {
		// 		str = fmt.Sprintf("%s[%v;doneSize:%v;status:%v]", str, c.cacheID, c.doneSize, c.status)
		// 	}
		// 	str = fmt.Sprintf("%s\n", str)
		// }

		return str, nil
	}

	d := s.dataManager.findData(cid)
	if d != nil {
		str = fmt.Sprintf("%scid:%v,totalSize:%v,cache:", str, d.cid, d.totalSize)
		for _, c := range d.cacheMap {
			str = fmt.Sprintf("%s[%v;doneSize:%v;status:%v]", str, c.cacheID, c.doneSize, c.status)
		}
		str = fmt.Sprintf("%s\n", str)

		return str, nil
	}

	return str, xerrors.New(ErrNodeNotFind)
}

// CacheBlocks Cache Block
func (s *Scheduler) CacheBlocks(ctx context.Context, cids []string, deviceID string) ([]string, error) {
	if len(cids) <= 0 {
		return nil, xerrors.New("cids is nil")
	}

	edge := s.nodeManager.getEdgeNode(deviceID)
	if edge != nil {
		errList := make([]string, 0)

		reqDatas, notFindList := edge.getReqCacheDatas(s.nodeManager, cids)
		for _, reqData := range reqDatas {
			err := edge.nodeAPI.CacheBlocks(ctx, reqData)
			if err != nil {
				log.Errorf("edge CacheData err:%v,url:%v,cids:%v", err.Error(), reqData.CandidateURL, reqData.Cids)
				errList = append(errList, reqData.CandidateURL)
			}
		}

		errList = append(errList, notFindList...)

		return errList, nil
	}

	candidate := s.nodeManager.getCandidateNode(deviceID)
	if candidate != nil {
		errList := make([]string, 0)

		reqDatas, _ := candidate.getReqCacheDatas(s.nodeManager, cids)
		for _, reqData := range reqDatas {
			err := candidate.nodeAPI.CacheBlocks(ctx, reqData)
			if err != nil {
				log.Errorf("candidate CacheData err:%v,url:%v,cids:%v", err.Error(), reqData.CandidateURL, reqData.Cids)
				errList = append(errList, reqData.CandidateURL)
			}
		}

		return errList, nil
	}

	return nil, xerrors.New(ErrNodeNotFind)
}

// InitNodeDeviceIDs Init Node DeviceIDs (test)
// func (s *Scheduler) InitNodeDeviceIDs(ctx context.Context) error {
// 	nodeNum := 1000

// 	edgePrefix := "edge_"
// 	candidatePrefix := "candidate_"

// 	edgeList := make([]string, 0)
// 	candidateList := make([]string, 0)
// 	for i := 0; i < nodeNum; i++ {
// 		edgeID := fmt.Sprintf("%s%d", edgePrefix, i)
// 		candidateID := fmt.Sprintf("%s%d", candidatePrefix, i)

// 		edgeList = append(edgeList, edgeID)
// 		candidateList = append(candidateList, candidateID)
// 	}

// 	err := cache.GetDB().SetEdgeDeviceIDList(edgeList)
// 	if err != nil {
// 		log.Errorf("SetEdgeDeviceIDList err:%v", err.Error())
// 	}

// 	err = cache.GetDB().SetCandidateDeviceIDList(candidateList)
// 	if err != nil {
// 		log.Errorf("SetCandidateDeviceIDList err:%v", err.Error())
// 	}

// 	return err
// }

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

// FindNodeWithBlock find node
func (s *Scheduler) FindNodeWithBlock(ctx context.Context, cid, ip string) (string, error) {
	// node, err := getNodeWithData(cid, ip)
	// if err != nil {
	// 	return "", err
	// }

	return "", nil
}

// GetDownloadInfoWithBlocks find node
func (s *Scheduler) GetDownloadInfoWithBlocks(ctx context.Context, cids []string, ip string) (map[string]api.DownloadInfo, error) {
	if len(cids) < 1 {
		return nil, xerrors.New("cids is nil")
	}

	geoInfo, err := region.GetRegion().GetGeoInfo(ip)
	if err != nil {
		log.Warnf("getNodeURLWithData GetGeoInfo err:%v,ip:%v", err, ip)
	}

	infoMap := make(map[string]api.DownloadInfo)

	for _, cid := range cids {
		info, err := s.nodeManager.findNodeDownloadInfo(cid, geoInfo)
		if err != nil {
			continue
		}

		infoMap[cid] = info
	}

	{
		// if len(infoMap) > 0 {
		// 	err := s.nodeManager.reward()
		// }
	}

	return infoMap, nil
}

// GetDownloadInfoWithBlock find node
func (s *Scheduler) GetDownloadInfoWithBlock(ctx context.Context, cid string, ip string) (api.DownloadInfo, error) {
	if cid == "" {
		return api.DownloadInfo{}, xerrors.New("cids is nil")
	}

	geoInfo, err := region.GetRegion().GetGeoInfo(ip)
	if err != nil {
		log.Warnf("getNodeURLWithData GetGeoInfo err:%v,ip:%v", err, ip)
	}

	return s.nodeManager.findNodeDownloadInfo(cid, geoInfo)
}

// CandidateNodeConnect Candidate connect
func (s *Scheduler) CandidateNodeConnect(ctx context.Context, url, token string) error {
	deviceID, err := verifySecret(token, api.NodeCandidate)
	if err != nil {
		log.Errorf("EdgeNodeConnect verifySecret err:%v", err)
		return err
	}

	t, err := s.AuthNew(ctx, api.AllPermissions)
	if err != nil {
		return xerrors.Errorf("creating auth token for remote connection: %w", err)
	}

	headers := http.Header{}
	headers.Add("Authorization", "Bearer "+string(t))
	// Connect to scheduler
	// log.Infof("EdgeNodeConnect edge url:%v", url)
	candicateAPI, closer, err := client.NewCandicate(ctx, url, headers)
	if err != nil {
		log.Errorf("CandidateNodeConnect NewCandicate err:%v,url:%v", err, url)
		return err
	}

	// load device info
	deviceInfo, err := candicateAPI.DeviceInfo(ctx)
	if err != nil {
		log.Errorf("CandidateNodeConnect DeviceInfo err:%v", err)
		return err
	}

	if deviceID != deviceInfo.DeviceId {
		return xerrors.Errorf("deviceID mismatch %s,%s", deviceID, deviceInfo.DeviceId)
	}

	candidateNode := &CandidateNode{
		nodeAPI: candicateAPI,
		closer:  closer,

		Node: Node{
			addr:       url,
			deviceInfo: deviceInfo,
		},
	}

	// ok, err := cache.GetDB().IsCandidateInDeviceIDList(deviceInfo.DeviceId)
	// if err != nil || !ok {
	// 	log.Errorf("EdgeNodeConnect IsCandidateInDeviceIDList err:%v,deviceID:%s", err, deviceInfo.DeviceId)
	// 	return xerrors.Errorf("deviceID does not exist")
	// }

	err = s.nodeManager.candidateOnline(candidateNode)
	if err != nil {
		log.Errorf("CandidateNodeConnect addEdgeNode err:%v,deviceID:%s", err, deviceInfo.DeviceId)
		return err
	}

	// cids := candidateNode.getCacheFailCids()
	// if cids != nil && len(cids) > 0 {
	// 	reqDatas, _ := candidateNode.getReqCacheDatas(s, cids, false)

	// 	for _, reqData := range reqDatas {
	// 		err := candidateNode.nodeAPI.CacheBlocks(ctx, reqData)
	// 		if err != nil {
	// 			log.Errorf("CandidateNodeConnect CacheData err:%v,url:%v,cids:%v", err.Error(), reqData.CandidateURL, reqData.Cids)
	// 		}
	// 	}
	// }

	return nil
}

// QueryCacheStatWithNode Query Cache Stat
func (s *Scheduler) QueryCacheStatWithNode(ctx context.Context, deviceID string) ([]api.CacheStat, error) {
	statList := make([]api.CacheStat, 0)

	// redis datas
	body := api.CacheStat{}
	count, err := persistent.GetDB().GetBlockNum(deviceID)
	if err == nil {
		body.CacheBlockCount = int(count)
	}

	statList = append(statList, body)

	// node datas
	candidata := s.nodeManager.getCandidateNode(deviceID)
	if candidata != nil {
		nodeBody, _ := candidata.nodeAPI.QueryCacheStat(ctx)
		statList = append(statList, nodeBody)
		return statList, nil
	}

	edge := s.nodeManager.getEdgeNode(deviceID)
	if edge != nil {
		nodeBody, _ := edge.nodeAPI.QueryCacheStat(ctx)
		statList = append(statList, nodeBody)
		return statList, nil
	}

	return statList, xerrors.New(ErrNodeNotFind)
}

// QueryCachingBlocksWithNode Query Caching Blocks
func (s *Scheduler) QueryCachingBlocksWithNode(ctx context.Context, deviceID string) (api.CachingBlockList, error) {
	candidata := s.nodeManager.getCandidateNode(deviceID)
	if candidata != nil {
		return candidata.nodeAPI.QueryCachingBlocks(ctx)
	}

	edge := s.nodeManager.getEdgeNode(deviceID)
	if edge != nil {
		return edge.nodeAPI.QueryCachingBlocks(ctx)
	}

	return api.CachingBlockList{}, xerrors.New(ErrNodeNotFind)
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
	candidate := s.nodeManager.getCandidateNode(deviceID)
	if candidate != nil {
		nodeInfo, err := candidate.getNodeInfo(deviceID)
		if err != nil {
			return api.DevicesInfo{}, err
		}
		candidate.deviceInfo.IpLocation = candidate.geoInfo.Geo
		candidate.deviceInfo.OnlineTime = (time.Minute * time.Duration(nodeInfo.OnlineTime)).String()
		candidate.deviceInfo.DeviceStatus = device.GetDeviceStatus(nodeInfo.IsOnline)
		return candidate.deviceInfo, nil
	}

	edge := s.nodeManager.getEdgeNode(deviceID)
	if edge != nil {
		nodeInfo, err := edge.getNodeInfo(deviceID)
		if err != nil {
			return api.DevicesInfo{}, err
		}
		edge.deviceInfo.IpLocation = edge.geoInfo.Geo
		edge.deviceInfo.OnlineTime = (time.Minute * time.Duration(nodeInfo.OnlineTime)).String()
		edge.deviceInfo.DeviceStatus = device.GetDeviceStatus(nodeInfo.IsOnline)
		return edge.deviceInfo, nil
	}

	return api.DevicesInfo{}, xerrors.New(ErrNodeNotFind)
}
