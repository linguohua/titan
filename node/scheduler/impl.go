package scheduler

import (
	"context"
	"fmt"

	logging "github.com/ipfs/go-log/v2"
	"golang.org/x/xerrors"

	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/api/client"
	"github.com/linguohua/titan/node/common"
	"github.com/linguohua/titan/node/scheduler/db"
)

var log = logging.Logger("scheduler")

// NewLocalScheduleNode NewLocalScheduleNode
func NewLocalScheduleNode() api.Scheduler {
	verifiedNodeMax := 10

	manager := newNodeManager()
	pool := newPoolGroup()
	election := newElection(verifiedNodeMax)
	validate := newValidate(verifiedNodeMax)

	s := &Scheduler{
		CommonAPI:   common.NewCommonAPI(manager.updateLastRequestTime),
		nodeManager: manager,
		poolGroup:   pool,
		election:    election,
		validate:    validate,
	}

	election.initElectionTimewheel(s)
	validate.initValidateTimewheel(s)

	return s
}

// Scheduler node
type Scheduler struct {
	common.CommonAPI

	nodeManager *NodeManager
	poolGroup   *PoolGroup

	election *Election
	validate *Validate
}

// EdgeNodeConnect edge connect
func (s *Scheduler) EdgeNodeConnect(ctx context.Context, url string) error {
	// Connect to scheduler
	// log.Infof("EdgeNodeConnect edge url:%v", url)
	edgeAPI, closer, err := client.NewEdge(ctx, url, nil)
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

	edgeNode := &EdgeNode{
		nodeAPI: edgeAPI,
		closer:  closer,

		Node: Node{
			addr:       url,
			deviceInfo: deviceInfo,
		},
	}

	ok, err := db.GetCacheDB().IsEdgeInDeviceIDList(deviceInfo.DeviceId)
	if err != nil || !ok {
		log.Errorf("EdgeNodeConnect IsEdgeInDeviceIDList err:%v,deviceID:%s", err, deviceInfo.DeviceId)
		return xerrors.Errorf("deviceID does not exist")
	}

	err = s.nodeManager.addEdgeNode(edgeNode)
	if err != nil {
		log.Errorf("EdgeNodeConnect addEdgeNode err:%v,deviceID:%s", err, deviceInfo.DeviceId)
		return err
	}

	s.poolGroup.addPendingNode(edgeNode, nil)

	cids := edgeNode.getCacheFailCids()
	if cids != nil && len(cids) > 0 {
		reqDatas, _ := edgeNode.getReqCacheDatas(s, cids, true)

		for _, reqData := range reqDatas {
			err := edgeNode.nodeAPI.CacheBlocks(ctx, reqData)
			if err != nil {
				log.Errorf("EdgeNodeConnect CacheData err:%v,url:%v,cids:%v", err.Error(), reqData.CandidateURL, reqData.Cids)
			}
		}
	}

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
func (s *Scheduler) CacheResult(ctx context.Context, deviceID string, info api.CacheResultInfo) error {
	edge := s.nodeManager.getEdgeNode(deviceID)
	if edge != nil {
		return edge.cacheBlockResult(&info)
	}

	candidate := s.nodeManager.getCandidateNode(deviceID)
	if candidate != nil {
		return candidate.cacheBlockResult(&info)
	}

	return xerrors.New("node not find")
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

	return nil, xerrors.New("node not find")
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
		return nil, xerrors.New("node not find")
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

// CacheBlocks Cache Block
func (s *Scheduler) CacheBlocks(ctx context.Context, cids []string, deviceID string) ([]string, error) {
	if len(cids) <= 0 {
		return nil, xerrors.New("cids is nil")
	}

	edge := s.nodeManager.getEdgeNode(deviceID)
	if edge != nil {
		errList := make([]string, 0)

		reqDatas, notFindList := edge.getReqCacheDatas(s, cids, true)
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

		reqDatas, _ := candidate.getReqCacheDatas(s, cids, false)
		for _, reqData := range reqDatas {
			err := candidate.nodeAPI.CacheBlocks(ctx, reqData)
			if err != nil {
				log.Errorf("candidate CacheData err:%v,url:%v,cids:%v", err.Error(), reqData.CandidateURL, reqData.Cids)
				errList = append(errList, reqData.CandidateURL)
			}
		}

		return errList, nil
	}

	return nil, xerrors.New("device not find")
}

// InitNodeDeviceIDs Init Node DeviceIDs (test)
func (s *Scheduler) InitNodeDeviceIDs(ctx context.Context) error {
	nodeNum := 1000

	edgePrefix := "edge_"
	candidatePrefix := "candidate_"

	edgeList := make([]string, 0)
	candidateList := make([]string, 0)
	for i := 0; i < nodeNum; i++ {
		edgeID := fmt.Sprintf("%s%d", edgePrefix, i)
		candidateID := fmt.Sprintf("%s%d", candidatePrefix, i)

		edgeList = append(edgeList, edgeID)
		candidateList = append(candidateList, candidateID)
	}

	err := db.GetCacheDB().SetEdgeDeviceIDList(edgeList)
	if err != nil {
		log.Errorf("SetEdgeDeviceIDList err:%v", err.Error())
	}

	err = db.GetCacheDB().SetCandidateDeviceIDList(candidateList)
	if err != nil {
		log.Errorf("SetCandidateDeviceIDList err:%v", err.Error())
	}

	return err
}

// GetOnlineDeviceIDs Get all online node id
func (s *Scheduler) GetOnlineDeviceIDs(ctx context.Context, nodeType api.NodeTypeName) ([]string, error) {
	list := make([]string, 0)

	if nodeType == api.TypeNameAll || nodeType == api.TypeNameCandidate || nodeType == api.TypeNameValidator {
		s.nodeManager.candidateNodeMap.Range(func(key, value interface{}) bool {
			deviceID := key.(string)
			if nodeType == api.TypeNameAll {
				list = append(list, deviceID)
			} else {
				node := value.(*CandidateNode)
				if (nodeType == api.TypeNameValidator) == node.isValidator {
					list = append(list, deviceID)
				}
			}

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

	return s.nodeManager.getDownloadInfoWithDatas(cids, ip)
}

// CandidateNodeConnect Candidate connect
func (s *Scheduler) CandidateNodeConnect(ctx context.Context, url string) error {
	candicateAPI, closer, err := client.NewCandicate(ctx, url, nil)
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

	candidateNode := &CandidateNode{
		nodeAPI: candicateAPI,
		closer:  closer,

		Node: Node{
			addr:       url,
			deviceInfo: deviceInfo,
		},
	}

	ok, err := db.GetCacheDB().IsCandidateInDeviceIDList(deviceInfo.DeviceId)
	if err != nil || !ok {
		log.Errorf("EdgeNodeConnect IsCandidateInDeviceIDList err:%v,deviceID:%s", err, deviceInfo.DeviceId)
		return xerrors.Errorf("deviceID does not exist")
	}

	err = s.nodeManager.addCandidateNode(candidateNode)
	if err != nil {
		log.Errorf("CandidateNodeConnect addEdgeNode err:%v,deviceID:%s", err, deviceInfo.DeviceId)
		return err
	}

	s.poolGroup.addPendingNode(nil, candidateNode)

	cids := candidateNode.getCacheFailCids()
	if cids != nil && len(cids) > 0 {
		reqDatas, _ := candidateNode.getReqCacheDatas(s, cids, false)

		for _, reqData := range reqDatas {
			err := candidateNode.nodeAPI.CacheBlocks(ctx, reqData)
			if err != nil {
				log.Errorf("CandidateNodeConnect CacheData err:%v,url:%v,cids:%v", err.Error(), reqData.CandidateURL, reqData.Cids)
			}
		}
	}

	return nil
}

// QueryCacheStatWithNode Query Cache Stat
func (s *Scheduler) QueryCacheStatWithNode(ctx context.Context, deviceID string) ([]api.CacheStat, error) {
	statList := make([]api.CacheStat, 0)

	// redis datas
	body := api.CacheStat{}
	count, err := db.GetCacheDB().GetCacheBlockNum(deviceID)
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

	return statList, xerrors.New("node not find")
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

	return api.CachingBlockList{}, xerrors.New("node not find")
}

// ElectionValidators Election Validators
func (s *Scheduler) ElectionValidators(ctx context.Context) error {
	return s.election.startElection(s)
}

// Validate Validate edge
func (s *Scheduler) Validate(ctx context.Context) error {
	return s.validate.startValidate(s)
}
