package scheduler

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"
	"time"

	logging "github.com/ipfs/go-log/v2"

	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/api/client"
	"github.com/linguohua/titan/node/common"
)

var log = logging.Logger("scheduler")

// NewLocalScheduleNode NewLocalScheduleNode
func NewLocalScheduleNode() api.Scheduler {
	return Scheduler{}
}

// Scheduler node
type Scheduler struct {
	common.CommonAPI
}

// EdgeNodeConnect edge connect
func (s Scheduler) EdgeNodeConnect(ctx context.Context, url string) error {
	// Connect to scheduler
	log.Infof("EdgeNodeConnect edge url : %v ", url)
	edgeAPI, closer, err := client.NewEdge(ctx, url, nil)
	if err != nil {
		log.Errorf("edgeAPI NewEdge err : %v", err)
		return err
	}

	// load device info
	deviceInfo, err := edgeAPI.DeviceInfo(ctx)
	if err != nil {
		log.Errorf("edgeAPI DeviceID err : %v", err)
		return err
	}

	log.Infof("edgeAPI Version deviceID : %v", deviceInfo.DeviceID)

	edgeNode := EdgeNode{
		addr:     url,
		edgeAPI:  edgeAPI,
		closer:   closer,
		deviceID: deviceInfo.DeviceID,
		userID:   url,
		ip:       "192.168.1.1", //"120.24.37.249", // TODO
	}
	addEdgeNode(&edgeNode)

	return nil
}

// CacheResult Cache Data Result
func (s Scheduler) CacheResult(ctx context.Context, deviceID string, cid string, isOK bool) error {
	return nodeCacheResult(deviceID, cid, isOK)
}

// CacheData Cache Data
func (s Scheduler) CacheData(ctx context.Context, cid, deviceID string) error {
	return CacheData(cid, deviceID)
}

// FindNodeWithData find node
func (s Scheduler) FindNodeWithData(ctx context.Context, cid, ip string) (string, error) {
	return GetNodeWithData(cid, ip)
}

// CandidateNodeConnect Candidate connect
func (s Scheduler) CandidateNodeConnect(ctx context.Context, url string) error {
	// candicateAPI, closer, err := client.NewCandicate(ctx, url, nil)
	// if err != nil {
	// 	log.Errorf("edgeAPI NewEdge err : %v", err)
	// 	return err
	// }

	// load device info
	// deviceID, err := candicateAPI.DeviceID(ctx)
	// if err != nil {
	// 	log.Errorf("edgeAPI DeviceID err : %v", err)
	// 	return err
	// }

	// log.Infof("edgeAPI Version deviceID : %v", deviceID)

	// edgeNode := EdgeNode{
	// 	addr:     url,
	// 	edgeAPI:  candicateAPI,
	// 	closer:   closer,
	// 	deviceID: deviceID,
	// 	userID:   url,
	// }
	// addEdgeNode(&edgeNode)
	return nil
}

// indexPage info
func (s Scheduler) GetIndexInfo(ctx context.Context, p api.IndexRequest) (api.IndexPageRes, error) {
	var dataRes api.IndexPageRes
	dataRes.StorageT = 1080.99
	dataRes.BandwidthMb = 666.99
	// AllMinerNum MinerInfo
	dataRes.AllCandidate = 18
	dataRes.AllEdgeNode = 122
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
func (s Scheduler) Retrieval(ctx context.Context, p api.IndexPageSearch) (api.RetrievalPageRes, error) {
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
	res.AllCandidate = 18
	res.AllEdgeNode = 122
	res.AllVerifier = 56
	return res, nil
}

func (s Scheduler) DevicesInfo(ctx context.Context, p api.DevicesSearch) (api.DevicesInfoPage, error) {
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

func (s Scheduler) DevicesCount(ctx context.Context, p api.DevicesSearch) (api.DeviceType, error) {
	var res api.DeviceType
	res.Online = 3
	res.Offline = 4
	res.Abnormal = 5
	res.AllDevices = 67
	// 需要同步数据
	res.BandwidthMb = 423327.22
	return res, nil
}

func (s Scheduler) GetDeviceDiagnosis(ctx context.Context, p api.DevicesSearch) (api.DeviceDiagnosis, error) {
	var res api.DeviceDiagnosis
	res.Secondary = 3
	res.Ordinary = 4
	res.Excellent = 5
	res.Good = 67
	// 需要同步数据
	res.DisGood = 4
	return res, nil
}

func (s Scheduler) GetDeviceDiagnosisDaily(ctx context.Context, p api.IncomeDailySearch) (api.IncomeDailyRes, error) {
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

func (s Scheduler) GetDeviceDiagnosisHour(ctx context.Context, p api.IncomeDailySearch) (api.HourDailyRes, error) {
	var res api.HourDailyRes
	m := timeFormatHour(p)
	res.OnlineJsonDaily = m
	res.DiskUsage = "127.2 G/447.1 G"
	return res, nil
}

// DevicesInfo search from mysql
func GetDevicesInfoList(info api.DevicesSearch) (list []api.DevicesInfo, total int64, err error) {
	//string转成int：
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
		p.Month = splitFrom[0] + "-" + splitFrom[1]
		getDaysData(strDaysFrom, p, &returnMapList)
	}
	// 当月数据查询
	p.Month = splitNow[0] + "-" + splitNow[1]
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
	//timeNow := time.Now().Format("2006-01-02")
	if p.DateFrom == "" && p.Date == "" && p.DateTo == "" {
		p.Date = "2022-06-30"
	}
	var returnMapList []interface{}
	// 单日数据
	if p.Date != "" {
		getHoursData([]string{}, p, &returnMapList)
	}
	return returnMapList
}

// getYearMonthToDay 查询指定年份指定月份有多少天
// @params year int 指定年份
// @params month int 指定月份
func getYearMonthToDay(year int, month int) int {
	// 有31天的月份
	day31 := map[int]struct{}{
		1:  struct{}{},
		3:  struct{}{},
		5:  struct{}{},
		7:  struct{}{},
		8:  struct{}{},
		10: struct{}{},
		12: struct{}{},
	}
	if _, ok := day31[month]; ok {
		return 31
	}
	// 有30天的月份
	day30 := map[int]struct{}{
		4:  struct{}{},
		6:  struct{}{},
		9:  struct{}{},
		11: struct{}{},
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
	month := strings.Split(p.Month, "-")[1]
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

func getHoursData(strDays []string, p api.IncomeDailySearch, returnMapList *[]interface{}) {
	//splitDate:=strings.Split(p.Date, "-")
	//p.Month = splitDate[1]
	//dayDate := splitDate[2]
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
	if info.UserId == "" || info.Month == "" {
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
	if info.Month != "" {
		db = db.Where("month = ?", info.Month)
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
