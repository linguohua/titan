package api

import (
	"time"

	"gorm.io/gorm"
)

// NodeTypeName node type
type NodeTypeName string

const (
	// TypeNameAll Edge
	TypeNameAll NodeTypeName = "All"
	// TypeNameEdge Edge
	TypeNameEdge NodeTypeName = "Edge"
	// TypeNameCandidate Candidate
	TypeNameCandidate NodeTypeName = "Candidate"
	// TypeNameValidator Validator
	TypeNameValidator NodeTypeName = "Validator"
)

type OpenRPCDocument map[string]interface{}

type IndexRequest struct {
	UserId string
}

// structure of index info
type IndexPageRes struct {
	// AllMinerNum MinerInfo
	AllMinerInfo
	// OnlineMinerNum MinerInfo
	OnlineVerifier  int `json:"online_verifier"`  // 在线验证人
	OnlineCandidate int `json:"online_candidate"` // 在线候选人
	OnlineEdgeNode  int `json:"online_edge_node"` // 在线边缘节点
	// ProfitInfo Profit  // 个人收益信息
	CumulativeProfit float64 `json:"cumulative_profit"` // 个人累计收益
	YesterdayProfit  float64 `json:"yesterday_profit"`  // 昨日收益
	TodayProfit      float64 `json:"today_profit"`      // 今日收益
	SevenDaysProfit  float64 `json:"seven_days_profit"` // 近七天收益
	MonthProfit      float64 `json:"month_profit"`      // 近30天收益
	// Device Devices // 设备信息
	TotalNum    int `json:"total_num"`    // 设备总数
	OnlineNum   int `json:"online_num"`   // 在线设备数
	OfflineNum  int `json:"offline_num"`  // 离线设备数
	AbnormalNum int `json:"abnormal_num"` // 异常设备数
}

type AllMinerInfo struct {
	AllVerifier  int     `json:"all_verifier"`  // 全网验证人
	AllCandidate int     `json:"all_candidate"` // 全网候选人
	AllEdgeNode  int     `json:"all_edgeNode"`  // 全网边缘节点
	StorageT     float64 `json:"storage_t"`     // 全网存储（T）
	BandwidthMb  float64 `json:"bandwidth_mb"`  // 全网上行带宽（MB/S）
}

type IndexPageSearch struct {
	RetrievalInfo
	PageInfo
}

// PageInfo Paging common input parameter structure
type PageInfo struct {
	Page     string `json:"page" form:"page"`         // 页码
	PageSize string `json:"pageSize" form:"pageSize"` // 每页大小
	Data     string `json:"data" form:"data"`         // 关键字
	DateFrom string `json:"dateFrom" form:"dateFrom"` // 日期开始
	DateTo   string `json:"dateTo" form:"dateTo"`     // 日期结束
	Date     string `json:"date" form:"date"`         // 具体日期
	Device   string `json:"deviceId" form:"deviceId"` // 设备ID
	UserIds  string `json:"userId" form:"userId"`     // 用户ID
	UserIp   string `json:"userIp" form:"userIp"`     // user ip address
}

// Retrieval miner info
type RetrievalInfo struct {
	ServiceCountry string  `json:"service_country"` // 服务商国家
	ServiceStatus  string  `json:"service_status"`  // 服务商网络状态
	TaskStatus     string  `json:"task_status"`     // 任务状态
	FileName       string  `json:"file_name"`       // 文件名
	FileSize       string  `json:"file_size"`       // 文件大小
	CreateTime     string  `json:"create_time"`     // 文件创建日期
	Cid            string  `json:"cid"`             // 编号
	Price          float64 `json:"price"`           // 价格
	MinerId        string  `json:"miner_id"`        // 矿工id
	UserId         string  `json:"user_id"`         // 用户id
	DownloadUrl    string  `json:"download_url"`    // download url address
}

// Response data of Retrieval miner info
type RetrievalPageRes struct {
	List []RetrievalInfo `json:"list"`
	AllMinerInfo
	Count int64 `json:"count"`
}

type DevicesSearch struct {
	DevicesInfo
	PageInfo
}
type GVAModel struct {
	ID        uint           `gorm:"primarykey"`                     // 主键ID
	CreatedAt time.Time      `gorm:"comment:'创建时间';type:timestamp;"` // 创建时间
	UpdatedAt time.Time      `gorm:"comment:'更新时间';type:timestamp;"` // 更新时间
	DeletedAt gorm.DeletedAt `gorm:"index" json:"-"`                 // 删除时间
}

// Devices Info
type DevicesInfo struct {
	GVAModel
	// 设备id
	DeviceId string `json:"deviceId" form:"deviceId" gorm:"column:device_id;comment:;"`
	// 设备名称
	DeviceName string `json:"deviceName" form:"deviceName" gorm:"column:device_name;comment:;"`
	// 用户id
	UserId string `json:"userId" form:"userId" gorm:"column:user_id;comment:;"`
	// sn码
	SnCode string `json:"snCode" form:"snCode" gorm:"column:sn_code;comment:;"`
	// 运营商
	Operator string `json:"operator" form:"operator" gorm:"column:operator;comment:;"`
	// 网络类型
	NetworkType string `json:"networkType" form:"networkType" gorm:"column:network_type;comment:;"`
	// 昨日收益
	YesterdayIncome *float64 `json:"yesterdayIncome" form:"yesterdayIncome" gorm:"column:yesterday_income;comment:;"`
	// 历史收益
	CumuProfit *float64 `json:"cumuProfit" form:"cumuProfit" gorm:"column:cumu_profit;comment:;"`
	// 基础信息
	// 系统版本
	SystemVersion string `json:"systemVersion" form:"systemVersion" gorm:"column:system_version;comment:;"`
	// 产品类型
	ProductType string `json:"productType" form:"productType" gorm:"column:product_type;comment:;"`
	// 网络信息
	NetworkInfo string `json:"networkInfo" form:"networkInfo" gorm:"column:network_info;comment:;"`
	// 外网ip
	ExternalIp string `json:"externalIp" form:"externalIp" gorm:"column:external_ip;comment:;"`
	// 内网ip
	InternalIp string `json:"internalIp" form:"internalIp" gorm:"column:internal_ip;comment:;"`
	// ip所属（地区）
	IpLocation string `json:"ipLocation" form:"ipLocation" gorm:"column:ip_location;comment:;"`
	// mac地址
	MacLocation string `json:"macLocation" form:"macLocation" gorm:"column:mac_location;comment:;"`
	// NAT类型
	NatType string `json:"natType" form:"natType" gorm:"column:nat_type;comment:;"`
	// UPNP
	Upnp string `json:"upnp" form:"upnp" gorm:"column:upnp;comment:;"`
	// 丢包率
	PkgLossRatio *float64 `json:"pkgLossRatio" form:"pkgLossRatio" gorm:"column:pkg_loss_ratio;comment:;"`
	// 时延
	Latency string `json:"latency" form:"latency" gorm:"column:latency;comment:;"`
	// 设备信息
	// CPU使用率
	CpuUsage string `json:"cpuUsage" form:"cpuUsage" gorm:"column:cpu_usage;comment:;"`
	// 内存使用率
	MemoryUsage string `json:"memoryUsage" form:"memoryUsage" gorm:"column:memory_usage;comment:;"`
	// 磁盘使用率
	DiskUsage string `json:"diskUsage" form:"diskUsage" gorm:"column:disk_usage;comment:;"`
	// 磁盘类型
	DiskType string `json:"diskType" form:"diskType" gorm:"column:disk_type;comment:;"`
	// 设备状态 online/offline/abnormal
	DeviceStatus string `json:"deviceStatus" form:"deviceStatus" gorm:"column:device_status;comment:;"`
	// 昨日诊断运行状态
	WorkStatus string `json:"workStatus" form:"workStatus" gorm:"column:work_status;comment:;"`
	// 文件系统
	IoSystem string `json:"ioSystem" form:"ioSystem" gorm:"column:io_system;comment:;"`
	// 今日在线时长
	TodayOnlineTime string `json:"todayOnlineTime" form:"todayOnlineTime" gorm:"column:today_online_time;comment:;"`
	// 额外字段非数据库
	TodayProfit     float64 `json:"today_profit"`      // 今日收益
	SevenDaysProfit float64 `json:"seven_days_profit"` // 近七天收益
	MonthProfit     float64 `json:"month_profit"`      // 近30天收益
	BandwidthUp     int64   `json:"bandwidth_up"`      // 上行带宽B/s
	BandwidthDown   int64   `json:"bandwidth_down"`    // 下行带宽B/s

}

// TableName IndexPage
func (DevicesInfo) TableName() string {
	return "devices_info"
}

type DevicesInfoPage struct {
	List  []DevicesInfo `json:"list"`
	Count int64         `json:"count"`
}

type DeviceType struct {
	Online      int64   `json:"online"`
	Offline     int64   `json:"offline"`
	Abnormal    int64   `json:"abnormal"`
	AllDevices  int64   `json:"allDevices"`
	BandwidthMb float64 `json:"bandwidth_mb"` // 全网上行带宽（MB/S）
}

type DeviceDiagnosis struct {
	Excellent int64 `json:"excellent"` // 优秀
	Good      int64 `json:"good"`      // 良好
	Secondary int64 `json:"secondary"` // 中等
	Ordinary  int64 `json:"ordinary"`  // 较差
	DisGood   int64 `json:"disGood"`   // 极差
}

type IncomeDailySearch struct {
	IncomeDaily
	PageInfo
}

type IncomeDaily struct {
	GVAModel
	// 用户id
	UserId string `json:"userId" form:"userId" gorm:"column:user_id;comment:;"`
	// 设备id
	DeviceId string `json:"deviceId" form:"deviceId" gorm:"column:device_id;comment:;"`
	// 月份
	DateStr string `json:"month" form:"month" gorm:"column:month;comment:;"`
	// 每日收益
	JsonDaily string `json:"jsonDaily" form:"jsonDaily" gorm:"column:json_daily;comment:;"`
	// 每日在线
	OnlineJsonDaily string `json:"onlineJsonDaily" form:"onlineJsonDaily" gorm:"column:online_daily;comment:;"`
	// 每日丢包率
	PkgLossRatio string `json:"pkgLossRatio" form:"pkgLossRatio" gorm:"column:pkg_loss_ratio;comment:;"`
	// 时延
	Latency string `json:"latency" form:"latency" gorm:"column:latency;comment:;"`
	// NAT类型
	NatType string `json:"natType" form:"natType" gorm:"column:nat_type;comment:;"`
	// 磁盘使用率
	DiskUsage string `json:"diskUsage" form:"diskUsage" gorm:"column:disk_usage;comment:;"`
}

// TableName IndexPage
func (IncomeDaily) TableName() string {
	return "income_daily_test"
}

type IncomeDailyRes struct {
	DailyIncome      interface{} `json:"daily_income"`      // 日常收益
	DefYesterday     string      `json:"def_yesterday"`     // 较昨日
	CumulativeProfit float64     `json:"cumulative_profit"` // 累计收益
	YesterdayProfit  float64     `json:"yesterday_profit"`  // 昨日收益
	SevenDaysProfit  float64     `json:"seven_days_profit"` // 近七天
	MonthProfit      float64     `json:"month_profit"`      // 近30天
	TodayProfit      float64     `json:"today_profit"`      // 今天收益
	OnlineTime       string      `json:"online_time"`       // 在线时长
	HighOnlineRatio  string      `json:"high_online_ratio"` // 高峰期在线率
}

type HourDailyRes struct {
	// 每日在线
	OnlineJsonDaily interface{} `json:"onlineJsonDaily" form:"onlineJsonDaily" gorm:"column:online_daily;comment:;"`
	//// 每日丢包率
	//PkgLossRatio  interface{} `json:"pkgLossRatio" form:"pkgLossRatio" gorm:"column:pkg_loss_ratio;comment:;"`
	//// 时延
	//Latency  interface{} `json:"latency" form:"latency" gorm:"column:latency;comment:;"`
	//// NAT类型
	//NatType  interface{} `json:"natType" form:"natType" gorm:"column:nat_type;comment:;"`
	// 磁盘使用率
	DiskUsage string `json:"diskUsage" form:"diskUsage" gorm:"column:disk_usage;comment:;"`
}

type HourDataOfDaily struct {
	GVAModel
	// 用户id
	UserId string `json:"userId" form:"userId" gorm:"column:user_id;comment:;"`
	// 设备id
	DeviceId string `json:"deviceId" form:"deviceId" gorm:"column:device_id;comment:;"`
	// 日期
	Date string `json:"date" form:"date" gorm:"column:date;comment:;"`
	// 每日在线
	OnlineJsonDaily string `json:"onlineJsonDaily" form:"onlineJsonDaily" gorm:"column:online_daily;comment:;"`
	// 每日丢包率
	PkgLossRatio string `json:"pkgLossRatio" form:"pkgLossRatio" gorm:"column:pkg_loss_ratio;comment:;"`
	// 时延
	Latency string `json:"latency" form:"latency" gorm:"column:latency;comment:;"`
	// NAT类型
	NatType string `json:"natType" form:"natType" gorm:"column:nat_type;comment:;"`
}

// TableName IndexPage
func (HourDataOfDaily) TableName() string {
	return "Hour_daily_test"
}

type IncomeJson31 struct {
	IncomeJson24
	TwentyFive  float64 `json:"25"`
	TwentySix   float64 `json:"26"`
	TwentySeven float64 `json:"27"`
	TwentyEight float64 `json:"28"`
	TwentyNine  float64 `json:"29"`
	Thirty      float64 `json:"30"`
	ThirtyOne   float64 `json:"31"`
}

type IncomeJson24 struct {
	IndexDays   int     `json:"index_days"`
	One         float64 `json:"01"`
	Two         float64 `json:"02"`
	Three       float64 `json:"03"`
	Four        float64 `json:"04"`
	Five        float64 `json:"05"`
	Six         float64 `json:"06"`
	Seven       float64 `json:"07"`
	Eight       float64 `json:"08"`
	Nine        float64 `json:"09"`
	Ten         float64 `json:"10"`
	Eleven      float64 `json:"11"`
	Twelve      float64 `json:"12"`
	Thirteen    float64 `json:"13"`
	Fourteen    float64 `json:"14"`
	Fifteen     float64 `json:"15"`
	Sixteen     float64 `json:"16"`
	Seventeen   float64 `json:"17"`
	Eighteen    float64 `json:"18"`
	Nineteen    float64 `json:"19"`
	Twenty      float64 `json:"20"`
	TwentyOne   float64 `json:"21"`
	TwentyTwo   float64 `json:"22"`
	TwentyThree float64 `json:"23"`
	TwentyFour  float64 `json:"24"`
}
