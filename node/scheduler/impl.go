package scheduler

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

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
	manager := newNodeManager()
	pool := newPoolGroup()
	validate := newElectionValidate()

	s := &Scheduler{
		CommonAPI:        common.NewCommonAPI(manager.updateLastRequestTime),
		nodeManager:      manager,
		poolGroup:        pool,
		electionValidate: validate,
	}

	validate.initValidateTimewheels(s)

	return s
}

// Scheduler node
type Scheduler struct {
	common.CommonAPI

	nodeManager *NodeManager
	poolGroup   *PoolGroup

	electionValidate *ElectionValidate
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

	list, err := edgeNode.getCacheFailCids()
	if err != nil {
		log.Warnf("EdgeNodeConnect getCacheFailCids err:%v,deviceID:%s", err, deviceInfo.DeviceId)
	} else {
		if list != nil && len(list) > 0 {
			reqDatas, _ := edgeNode.getReqCacheDatas(s, list, true)
			for _, reqData := range reqDatas {
				err := edgeNode.nodeAPI.CacheData(ctx, reqData)
				if err != nil {
					log.Errorf("EdgeNodeConnect CacheData err:%v,url:%v,cids:%v", err.Error(), reqData.CandidateURL, reqData.Cids)
				}
			}
		}
	}

	return nil
}

// ValidateDataResult Validate Data Result
func (s Scheduler) ValidateDataResult(ctx context.Context, validateResults api.ValidateResults) error {
	err := s.electionValidate.validateResult(&validateResults)
	if err != nil {
		log.Errorf("ValidateDataResult err:%v", err.Error())
	}

	return err
}

// CacheResult Cache Data Result
func (s *Scheduler) CacheResult(ctx context.Context, deviceID string, info api.CacheResultInfo) (string, error) {
	edge := s.nodeManager.getEdgeNode(deviceID)
	if edge != nil {
		return edge.nodeCacheResult(&info)
	}

	candidate := s.nodeManager.getCandidateNode(deviceID)
	if candidate != nil {
		return candidate.nodeCacheResult(&info)
	}

	return "", xerrors.New("node not find")
}

// DeleteDataRecords  Delete Data Record
func (s *Scheduler) DeleteDataRecords(ctx context.Context, deviceID string, cids []string) (map[string]string, error) {
	if len(cids) <= 0 {
		return nil, xerrors.New("cids is nil")
	}

	edge := s.nodeManager.getEdgeNode(deviceID)
	if edge != nil {
		return edge.deleteDataRecords(cids)
	}

	candidate := s.nodeManager.getCandidateNode(deviceID)
	if candidate != nil {
		return candidate.deleteDataRecords(cids)
	}

	return nil, xerrors.New("node not find")
}

// DeleteDatas  Delete Datas
func (s *Scheduler) DeleteDatas(ctx context.Context, deviceID string, cids []string) (map[string]string, error) {
	if len(cids) <= 0 {
		return nil, xerrors.New("cids is nil")
	}

	errorMap := make(map[string]string)

	nodeFinded := false

	var node Node

	edge := s.nodeManager.getEdgeNode(deviceID)
	if edge != nil {
		result, err := edge.nodeAPI.DeleteData(ctx, cids)
		if err != nil {
			return nil, err
		}

		nodeFinded = true

		if len(result.List) > 0 {
			for _, data := range result.List {
				errorMap[data.Cid] = data.ErrMsg
			}
		}

		node = edge.Node
	}

	candidate := s.nodeManager.getCandidateNode(deviceID)
	if candidate != nil {
		result, err := candidate.nodeAPI.DeleteData(ctx, cids)
		if err != nil {
			return nil, err
		}

		nodeFinded = true

		if len(result.List) > 0 {
			for _, data := range result.List {
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

	eList, err := node.deleteDataRecords(delRecordList)
	for cid, eSrt := range eList {
		errorMap[cid] = eSrt
	}

	return errorMap, err
}

// CacheDatas Cache Data
func (s *Scheduler) CacheDatas(ctx context.Context, cids []string, deviceID string) ([]string, error) {
	if len(cids) <= 0 {
		return nil, xerrors.New("cids is nil")
	}

	edge := s.nodeManager.getEdgeNode(deviceID)
	if edge != nil {
		errList := make([]string, 0)

		reqDatas, notFindList := edge.getReqCacheDatas(s, cids, true)
		for _, reqData := range reqDatas {
			err := edge.nodeAPI.CacheData(ctx, reqData)
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
			err := candidate.nodeAPI.CacheData(ctx, reqData)
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

// GetCacheTag get a tag with cid
func (s *Scheduler) GetCacheTag(ctx context.Context, cid, deviceID string) (string, error) {
	edge := s.nodeManager.getEdgeNode(deviceID)
	if edge != nil {
		return edge.newCacheDataTag(cid)
	}

	candidate := s.nodeManager.getCandidateNode(deviceID)
	if candidate != nil {
		return candidate.newCacheDataTag(cid)
	}

	return "", xerrors.New("device not find")
}

// FindNodeWithData find node
func (s *Scheduler) FindNodeWithData(ctx context.Context, cid, ip string) (string, error) {
	// node, err := getNodeWithData(cid, ip)
	// if err != nil {
	// 	return "", err
	// }

	return "", nil
}

// GetDownloadURLWithData find node
func (s *Scheduler) GetDownloadURLWithData(ctx context.Context, cid, ip string) (string, error) {
	if cid == "" {
		return "", xerrors.New("cid is nil")
	}

	return s.nodeManager.getNodeURLWithData(cid, ip)
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

	list, err := candidateNode.getCacheFailCids()
	if err != nil {
		log.Warnf("CandidateNodeConnect getCacheFailCids err:%v,deviceID:%s", err, deviceInfo.DeviceId)
	} else {
		if list != nil && len(list) > 0 {
			reqDatas, _ := candidateNode.getReqCacheDatas(s, list, false)
			for _, reqData := range reqDatas {
				err := candidateNode.nodeAPI.CacheData(ctx, reqData)
				if err != nil {
					log.Errorf("CandidateNodeConnect CacheData err:%v,url:%v,cids:%v", err.Error(), reqData.CandidateURL, reqData.Cids)
				}
			}
		}
	}

	return nil
}

// QueryCacheStatWithNode Query Cache Stat
func (s *Scheduler) QueryCacheStatWithNode(ctx context.Context, deviceID string) ([]api.CacheStat, error) {
	stats := make([]api.CacheStat, 0)

	// redis datas
	body := api.CacheStat{}
	infos, err := db.GetCacheDB().GetCacheDataInfos(deviceID)
	if err == nil && len(infos) > 0 {
		count := 0
		for _, tag := range infos {
			if tag != dataDefaultTag {
				count++
			}
		}
		body.CacheBlockCount = count
	}

	stats = append(stats, body)

	// node datas
	candidata := s.nodeManager.getCandidateNode(deviceID)
	if candidata != nil {
		nodeBody, _ := candidata.nodeAPI.QueryCacheStat(ctx)
		stats = append(stats, nodeBody)
		return stats, nil
	}

	edge := s.nodeManager.getEdgeNode(deviceID)
	if edge != nil {
		nodeBody, _ := edge.nodeAPI.QueryCacheStat(ctx)
		stats = append(stats, nodeBody)
		return stats, nil
	}

	return stats, xerrors.New("node not find")
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
	return s.electionValidate.electionValidators(s)
}

// Validate Validate edge
func (s *Scheduler) Validate(ctx context.Context) error {
	return s.electionValidate.startValidates(s)
}

// indexPage info
func (s *Scheduler) GetIndexInfo(ctx context.Context, p api.IndexRequest) (api.IndexPageRes, error) {
	var dataRes api.IndexPageRes
	dataRes.StorageT = 1080.99
	dataRes.BandwidthMb = 666.99
	// AllMinerNum MinerInfo
	dataRes.AllCandidate = s.nodeManager.candidateCount
	dataRes.AllEdgeNode = s.nodeManager.edgeCount
	dataRes.AllVerifier = 56
	// OnlineMinerNum MinerInfo
	dataRes.OnlineCandidate = 11
	dataRes.OnlineEdgeNode = 252
	dataRes.OnlineVerifier = 88
	// Devices
	dataRes.AbnormalNum = 12
	dataRes.OfflineNum = 12
	dataRes.OnlineNum = 12
	dataRes.TotalNum = 36
	// Profit
	dataRes.CumulativeProfit = 36
	dataRes.MonthProfit = 36
	dataRes.SevenDaysProfit = 36
	dataRes.YesterdayProfit = 36
	return dataRes, nil
}

// Retrieval miner info
func (s *Scheduler) Retrieval(ctx context.Context, p api.IndexPageSearch) (api.RetrievalPageRes, error) {
	var res api.RetrievalPageRes
	var dataRes api.RetrievalInfo
	var dataList []api.RetrievalInfo
	dataRes.Price = 108.99
	dataRes.ServiceCountry = "china"
	// AllMinerNum MinerInfo
	dataRes.Cid = "noas9878as88as8"
	dataRes.CreateTime = "2022-01-22"
	dataRes.FileName = "文件一"
	dataRes.FileSize = "12kb"
	dataRes.ServiceStatus = "可用"
	dataList = append(dataList, dataRes)
	dataList = append(dataList, dataRes)
	dataList = append(dataList, dataRes)
	res.List = dataList
	res.Count = 3
	// 后续通过调度器动态获取
	res.StorageT = 1080.99
	res.BandwidthMb = 666.99
	// AllMinerNum MinerInfo
	res.AllCandidate = s.nodeManager.candidateCount
	res.AllEdgeNode = s.nodeManager.edgeCount
	res.AllVerifier = 56
	return res, nil
}

func (s *Scheduler) GetDevicesInfo(ctx context.Context, p api.DevicesSearch) (api.DevicesInfoPage, error) {
	var res api.DevicesInfoPage
	list, total, err := GetDevicesInfoList(p)
	if err != nil {
		return res, err
	}
	var dataList []api.DevicesInfo
	for _, data := range list {
		err = getProfitByDeviceId(&data)
		if err != nil {
			log.Error("getProfitByDeviceId：", data.DeviceId)
		}
		dataList = append(dataList, data)
	}
	res.List = dataList
	res.Count = total
	return res, nil
}

func (s *Scheduler) GetDevicesCount(ctx context.Context, p api.DevicesSearch) (api.DeviceType, error) {
	var res api.DeviceType
	res.Online = 3
	res.Offline = 4
	res.Abnormal = 5
	res.AllDevices = 67
	// 需要同步数据
	res.BandwidthMb = 423327.22
	return res, nil
}

func (s *Scheduler) GetDeviceDiagnosis(ctx context.Context, p api.DevicesSearch) (api.DeviceDiagnosis, error) {
	var res api.DeviceDiagnosis
	res.Secondary = 3
	res.Ordinary = 4
	res.Excellent = 5
	res.Good = 67
	// 需要同步数据
	res.DisGood = 4
	return res, nil
}

func (s *Scheduler) GetDeviceDiagnosisDaily(ctx context.Context, p api.IncomeDailySearch) (api.IncomeDailyRes, error) {
	var res api.IncomeDailyRes
	m := timeFormat(p)
	res.DailyIncome = m
	res.DefYesterday = "31.1%"
	res.YesterdayProfit = 12.33
	res.SevenDaysProfit = 32.33
	res.CumulativeProfit = 212.33
	res.MonthProfit = 112.33
	return res, nil
}

func (s *Scheduler) GetDeviceDiagnosisHour(ctx context.Context, p api.IncomeDailySearch) (api.HourDailyRes, error) {
	var res api.HourDailyRes
	m := timeFormatHour(p)
	res.OnlineJsonDaily = m
	res.DiskUsage = "127.2 G/447.1 G"
	return res, nil
}

//  dairy data save
func (s *Scheduler) SaveDailyInfo(ctx context.Context, incomeDaily api.IncomeDaily) error {
	splitDate := strings.Split(incomeDaily.DateStr, "-")
	month := splitDate[0] + "-" + splitDate[1]
	dayDate := splitDate[2]
	var incomeDailyOld api.IncomeDaily
	result := db.GMysqlDb.Where("device_id = ?", incomeDaily.DeviceId).Where("user_id = ?", incomeDaily.UserId).
		Where("month = ?", month).First(&incomeDailyOld)
	incomeDailyOld.DiskUsage = setDailyInfo(incomeDailyOld.DiskUsage, dayDate, incomeDaily.DiskUsage)
	incomeDailyOld.Latency = setDailyInfo(incomeDailyOld.Latency, dayDate, incomeDaily.Latency)
	incomeDailyOld.JsonDaily = setDailyInfo(incomeDailyOld.JsonDaily, dayDate, incomeDaily.JsonDaily)
	incomeDailyOld.NatType = setDailyInfo(incomeDailyOld.NatType, dayDate, incomeDaily.NatType)
	incomeDailyOld.PkgLossRatio = setDailyInfo(incomeDailyOld.PkgLossRatio, dayDate, incomeDaily.PkgLossRatio)
	incomeDailyOld.OnlineJsonDaily = setDailyInfo(incomeDailyOld.OnlineJsonDaily, dayDate, incomeDaily.OnlineJsonDaily)
	incomeDailyOld.DateStr = month
	incomeDailyOld.DeviceId = incomeDaily.DeviceId
	incomeDailyOld.UserId = incomeDaily.UserId
	if result.RowsAffected <= 0 {
		err := db.GMysqlDb.Create(&incomeDailyOld).Error
		return err
	} else {
		err := db.GMysqlDb.Save(&incomeDailyOld).Error
		return err
	}
}

// DevicesInfo search from mysql
func GetDevicesInfoList(info api.DevicesSearch) (list []api.DevicesInfo, total int64, err error) {
	// string转成int：
	limit, _ := strconv.Atoi(info.PageSize)
	page, _ := strconv.Atoi(info.Page)
	offset := limit * (page - 1)
	// 创建db
	db := db.GMysqlDb.Model(&api.DevicesInfo{})
	var InPages []api.DevicesInfo
	// 如果有条件搜索 下方会自动创建搜索语句
	if info.DeviceId != "" {
		db = db.Where("device_id = ?", info.DeviceId)
	}
	// 如果有条件搜索 下方会自动创建搜索语句
	if info.UserId != "" {
		db = db.Where("user_id = ?", info.UserId)
	}
	// 如果有条件搜索 下方会自动创建搜索语句
	if info.DeviceStatus != "" && info.DeviceStatus != "allDevices" {
		db = db.Where("device_status = ?", info.DeviceStatus)
	}
	err = db.Count(&total).Error
	if err != nil {
		return
	}
	err = db.Limit(limit).Offset(offset).Find(&InPages).Error
	return InPages, total, err
}

// 此处模拟的每台设备累计数据
func getProfitByDeviceId(rt *api.DevicesInfo) error {
	deviceId := rt.DeviceId
	// 这里需要获取设备当日在线时长写入map
	mapTime := make(map[string]string)
	mapTime["122"] = "5.6h"
	mapTime["123"] = "9.7h"
	mapTime["124"] = "5.43h"
	mapTime["125"] = "7.63h"
	mapTime["126"] = "15.6h"
	todayOnlineTime := mapTime[deviceId]
	// 根据deviceId配置在线时长
	rt.TodayOnlineTime = todayOnlineTime
	// 根据deviceId配置今天收益
	rt.TodayProfit = 4.99
	// 根据deviceId配置7天收益
	rt.SevenDaysProfit = 23.99
	// 根据deviceId配置30天收益
	rt.MonthProfit = 344.99
	return nil
}

func timeFormat(p api.IncomeDailySearch) (m []interface{}) {
	timeNow := time.Now().Format("2006-01-02")
	var returnMapList []interface{}
	// 默认两周的数据
	dd, _ := time.ParseDuration("-24h")
	FromTime := time.Now().Add(dd * 14).Format("2006-01-02")
	if p.DateFrom == "" && p.Date == "" {
		p.DateFrom = FromTime
	}
	if p.DateTo == "" && p.Date == "" {
		p.DateTo = timeNow
	}
	splitNow := strings.Split(p.DateTo, "-")
	splitFrom := strings.Split(p.DateFrom, "-")
	day, err := strconv.Atoi(splitNow[2])
	if err != nil {
		return
	}
	var strDaysTo []string
	if splitNow[1] != splitFrom[1] {
		year, err := strconv.Atoi(splitFrom[0])
		if err != nil {
			return
		}
		month, err := strconv.Atoi(splitFrom[1])
		if err != nil {
			return
		}
		days := getYearMonthToDay(year, month)
		day, err := strconv.Atoi(splitFrom[2])
		if err != nil {
			return
		}
		var strDaysFrom []string

		for i := day; i <= days; i++ {
			stringFrom := strconv.Itoa(i)
			if len(stringFrom) < 2 {
				stringFrom = "0" + stringFrom
			}
			strDaysFrom = append(strDaysFrom, stringFrom)
		}
		// 当月数据查询
		p.DateStr = splitFrom[0] + "-" + splitFrom[1]
		getDaysData(strDaysFrom, p, &returnMapList)
	}
	// 当月数据查询
	p.DateStr = splitNow[0] + "-" + splitNow[1]
	for i := 1; i < day; i++ {
		stringTo := strconv.Itoa(i)
		if len(stringTo) < 2 {
			stringTo = "0" + stringTo
		}
		strDaysTo = append(strDaysTo, stringTo)
	}
	getDaysData(strDaysTo, p, &returnMapList)
	return returnMapList
}

func timeFormatHour(p api.IncomeDailySearch) (m []interface{}) {
	// timeNow := time.Now().Format("2006-01-02")
	if p.DateFrom == "" && p.Date == "" && p.DateTo == "" {
		p.Date = "2022-06-30"
	}
	var returnMapList []interface{}
	// 单日数据
	if p.Date != "" {
		getHoursData(p, &returnMapList)
	}
	return returnMapList
}

// getYearMonthToDay 查询指定年份指定月份有多少天
// @params year int 指定年份
// @params month int 指定月份
func getYearMonthToDay(year int, month int) int {
	// 有31天的月份
	day31 := map[int]struct{}{
		1:  {},
		3:  {},
		5:  {},
		7:  {},
		8:  {},
		10: {},
		12: {},
	}
	if _, ok := day31[month]; ok {
		return 31
	}
	// 有30天的月份
	day30 := map[int]struct{}{
		4:  {},
		6:  {},
		9:  {},
		11: {},
	}
	if _, ok := day30[month]; ok {
		return 30
	}
	// 计算是平年还是闰年
	if (year%4 == 0 && year%100 != 0) || year%400 == 0 {
		// 得出2月的天数
		return 29
	}
	// 得出2月的天数
	return 28
}

func getDaysData(strDays []string, p api.IncomeDailySearch, returnMapList *[]interface{}) {
	list, _, err := GetIncomeDailyList(p)
	if err != nil {
		return
	}
	queryMapFrom := make(map[string]interface{})
	e := json.Unmarshal([]byte(list.JsonDaily), &queryMapFrom)
	if e != nil {
		return
	}
	onlineJsonDailyFrom := make(map[string]interface{})
	e = json.Unmarshal([]byte(list.OnlineJsonDaily), &onlineJsonDailyFrom)
	if e != nil {
		return
	}
	pkgLossRatioFrom := make(map[string]interface{})
	e = json.Unmarshal([]byte(list.PkgLossRatio), &pkgLossRatioFrom)
	if e != nil {
		return
	}
	latencyFrom := make(map[string]interface{})
	e = json.Unmarshal([]byte(list.Latency), &latencyFrom)
	if e != nil {
		return
	}
	natTypeFrom := make(map[string]interface{})
	e = json.Unmarshal([]byte(list.NatType), &natTypeFrom)
	if e != nil {
		return
	}
	diskUsageFrom := make(map[string]interface{})
	e = json.Unmarshal([]byte(list.DiskUsage), &diskUsageFrom)
	if e != nil {
		return
	}
	month := strings.Split(p.DateStr, "-")[1]
	for _, v := range strDays {
		returnMap := make(map[string]interface{})
		returnMap["date"] = month + "-" + v
		returnMap["income"] = queryMapFrom[v]
		returnMap["online"] = onlineJsonDailyFrom[v]
		returnMap["pkgLoss"] = pkgLossRatioFrom[v]
		returnMap["latency"] = latencyFrom[v]
		returnMap["natType"] = natTypeFrom[v]
		returnMap["diskUsage"] = diskUsageFrom[v]
		*returnMapList = append(*returnMapList, returnMap)
	}
	return
}

func getHoursData(p api.IncomeDailySearch, returnMapList *[]interface{}) {
	listHour, _, err := GetHourDailyList(p)
	if err != nil {
		return
	}
	onlineJsonDailyFrom := make(map[string]interface{})
	e := json.Unmarshal([]byte(listHour.OnlineJsonDaily), &onlineJsonDailyFrom)
	if e != nil {
		return
	}
	pkgLossRatioFrom := make(map[string]interface{})
	e = json.Unmarshal([]byte(listHour.PkgLossRatio), &pkgLossRatioFrom)
	if e != nil {
		return
	}
	latencyFrom := make(map[string]interface{})
	e = json.Unmarshal([]byte(listHour.Latency), &latencyFrom)
	if e != nil {
		return
	}
	natTypeFrom := make(map[string]interface{})
	e = json.Unmarshal([]byte(listHour.NatType), &natTypeFrom)
	if e != nil {
		return
	}
	for i := 1; i <= 24; i++ {
		stringFrom := strconv.Itoa(i)
		if len(stringFrom) < 2 {
			stringFrom = "0" + stringFrom
		}
		returnMap := make(map[string]interface{})
		returnMap["date"] = stringFrom + ":00"
		returnMap["online"] = onlineJsonDailyFrom[stringFrom]
		returnMap["pkgLoss"] = pkgLossRatioFrom[stringFrom]
		returnMap["latency"] = latencyFrom[stringFrom]
		returnMap["natType"] = natTypeFrom[stringFrom]
		*returnMapList = append(*returnMapList, returnMap)
	}
	return
}

func GetIncomeDailyList(info api.IncomeDailySearch) (list api.IncomeDaily, total int64, err error) {
	// 创建db
	db := db.GMysqlDb.Model(&api.IncomeDaily{})
	var InPages api.IncomeDaily
	// 如果有条件搜索 下方会自动创建搜索语句
	if info.UserId == "" || info.DateStr == "" {
		log.Error("参数错误")
		return
	}
	// 如果有条件搜索 下方会自动创建搜索语句
	if info.DeviceId != "" {
		db = db.Where("device_id = ?", info.DeviceId)
	}
	// 如果有条件搜索 下方会自动创建搜索语句
	if info.UserId != "" {
		db = db.Where("user_id = ?", info.UserId)
	}
	// 如果有条件搜索 下方会自动创建搜索语句
	if info.DateStr != "" {
		db = db.Where("month = ?", info.DateStr)
	}
	err = db.Count(&total).Error
	if err != nil {
		return
	}
	err = db.Find(&InPages).First(&InPages).Error
	return InPages, total, err
}

func GetHourDailyList(info api.IncomeDailySearch) (list api.HourDataOfDaily, total int64, err error) {
	// 创建db
	db := db.GMysqlDb.Model(&api.HourDataOfDaily{})
	var InPages api.HourDataOfDaily
	// 如果有条件搜索 下方会自动创建搜索语句
	if info.UserId == "" || info.Date == "" {
		log.Error("参数错误")
		return
	}
	// 如果有条件搜索 下方会自动创建搜索语句
	if info.DeviceId != "" {
		db = db.Where("device_id = ?", info.DeviceId)
	}
	// 如果有条件搜索 下方会自动创建搜索语句
	if info.UserId != "" {
		db = db.Where("user_id = ?", info.UserId)
	}
	// 如果有条件搜索 下方会自动创建搜索语句
	if info.Date != "" {
		db = db.Where("date = ?", info.Date)
	}
	err = db.Count(&total).Error
	if err != nil {
		return
	}
	err = db.Find(&InPages).First(&InPages).Error
	return InPages, total, err
}

func setDailyInfo(jsonStr, date, income string) string {
	onlineJsonDailyFrom := make(map[string]interface{})
	if jsonStr != "" {
		e := json.Unmarshal([]byte(jsonStr), &onlineJsonDailyFrom)
		if e != nil {
			return ""
		}
	}
	if income == "" {
		income = "0"
	}
	onlineJsonDailyFrom[date] = income
	bytes, e := json.Marshal(onlineJsonDailyFrom)
	if e != nil {
		return ""
	}
	jsonString := string(bytes)
	return jsonString
}
