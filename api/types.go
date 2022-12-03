package api

import (
	"time"
)

// NodeTypeName node type
type NodeTypeName string

const (
	// TypeNameAll Edge
	TypeNameAll NodeTypeName = "all"
	// TypeNameEdge Edge
	TypeNameEdge NodeTypeName = "edge"
	// TypeNameCandidate Candidate
	TypeNameCandidate NodeTypeName = "candidate"
	// TypeNameValidator Validator
	TypeNameValidator NodeTypeName = "validator"
)

type OpenRPCDocument map[string]interface{}

type Base struct {
	ID        uint      `gorm:"primarykey"`
	CreatedAt time.Time `json:"created_at" gorm:"comment:'创建时间';type:timestamp;"`
	UpdatedAt time.Time `json:"updated_at" gorm:"comment:'更新时间';type:timestamp;"`
}

// DevicesInfo Info
type DevicesInfo struct {
	Base
	NodeType        NodeType `redis:"NodeType"`
	DeviceId        string   `json:"device_id" form:"deviceId" gorm:"column:device_id;comment:;" redis:"DeviceId"`
	DeviceName      string   `json:"device_name" form:"deviceName" gorm:"column:device_name;comment:;" redis:"DeviceName"`
	UserId          string   `json:"user_id" form:"userId" gorm:"column:user_id;comment:;"`
	SnCode          string   `json:"sn_code" form:"snCode" gorm:"column:sn_code;comment:;"`
	Operator        string   `json:"operator" form:"operator" gorm:"column:operator;comment:;" redis:"Operator"`
	NetworkType     string   `json:"network_type" form:"networkType" gorm:"column:network_type;comment:;" redis:"NetworkType"`
	SystemVersion   string   `json:"system_version" form:"systemVersion" gorm:"column:system_version;comment:;" redis:"SystemVersion"`
	ProductType     string   `json:"product_type" form:"productType" gorm:"column:product_type;comment:;" redis:"ProductType"`
	NetworkInfo     string   `json:"network_info" form:"networkInfo" gorm:"column:network_info;comment:;" redis:"NetworkInfo"`
	ExternalIp      string   `json:"external_ip" form:"externalIp" gorm:"column:external_ip;comment:;" redis:"ExternalIp"`
	InternalIp      string   `json:"internal_ip" form:"internalIp" gorm:"column:internal_ip;comment:;" redis:"InternalIp"`
	IpLocation      string   `json:"ip_location" form:"ipLocation" gorm:"column:ip_location;comment:;" redis:"IpLocation"`
	MacLocation     string   `json:"mac_location" form:"macLocation" gorm:"column:mac_location;comment:;" redis:"MacLocation"`
	NatType         string   `json:"nat_type" form:"natType" gorm:"column:nat_type;comment:;" redis:"NatType"`
	Upnp            string   `json:"upnp" form:"upnp" gorm:"column:upnp;comment:;" redis:"Upnp"`
	PkgLossRatio    float64  `json:"pkg_loss_ratio" form:"pkgLossRatio" gorm:"column:pkg_loss_ratio;comment:;" redis:"PkgLossRatio"`
	Latency         float64  `json:"latency" form:"latency" gorm:"column:latency;comment:;" redis:"Latency"`
	CpuUsage        float64  `json:"cpu_usage" form:"cpuUsage" gorm:"column:cpu_usage;comment:;" redis:"CpuUsage"`
	CPUCores        int      `json:"cpu_cores" form:"cpuCores" gorm:"column:cpu_cores;comment:;" redis:"cpuCores"`
	MemoryUsage     float64  `json:"memory_usage" form:"memoryUsage" gorm:"column:memory_usage;comment:;" redis:"MemoryUsage"`
	Memory          float64  `json:"memory" form:"memory" gorm:"column:memory;comment:;" redis:"Memory"`
	DiskUsage       float64  `json:"disk_usage" form:"diskUsage" gorm:"column:disk_usage;comment:;" redis:"DiskUsage"`
	DiskSpace       float64  `json:"disk_space" form:"diskSpace" gorm:"column:disk_space;comment:;" redis:"DiskSpace"`
	DiskType        string   `json:"disk_type" form:"diskType" gorm:"column:disk_type;comment:;" redis:"DiskType"`
	DeviceStatus    string   `json:"device_status" form:"deviceStatus" gorm:"column:device_status;comment:;" redis:"DeviceStatus"`
	WorkStatus      string   `json:"work_status" form:"workStatus" gorm:"column:work_status;comment:;" redis:"WorkStatus"`
	IoSystem        string   `json:"io_system" form:"ioSystem" gorm:"column:io_system;comment:;" redis:"IoSystem"`
	TodayOnlineTime float64  `json:"today_online_time" form:"todayOnlineTime" gorm:"column:today_online_time;comment:;" redis:"TodayOnlineTime"`
	NatRatio        float64  `json:"nat_ratio" form:"nat_ratio" gorm:"column:nat_ratio;comment:;" redis:"NatRatio"`
	OnlineTime      float64  `json:"online_time" form:"OnlineTime" redis:"OnlineTime"`
	TodayProfit     float64  `json:"today_profit" redis:"TodayProfit"`
	LastRewardDate  string   `json:"-" redis:"LastRewardDate"`
	BandwidthUp     float64  `json:"bandwidth_up" redis:"BandwidthUp"`
	BandwidthDown   float64  `json:"bandwidth_down" redis:"BandwidthDown"`
	TotalDownload   float64  `json:"total_download" redis:"TotalDownload"`
	TotalUpload     float64  `json:"total_upload" redis:"TotalUpload"`
}

type BlockDownloadInfo struct {
	ID           string    `json:"-"`
	DeviceID     string    `json:"device_id" db:"device_id"`
	BlockCID     string    `json:"block_cid" db:"block_cid"`
	BlockSize    int       `json:"block_size" db:"block_size"`
	Speed        int64     `json:"speed" db:"speed"`
	Reward       int64     `json:"reward" db:"reward"`
	Status       int       `json:"status" db:"status"`
	FailedReason string    `json:"failed_reason" db:"failed_reason"`
	ClientIP     string    `json:"client_ip" db:"client_ip"`
	CreatedTime  time.Time `json:"created_time" db:"created_time"`
	CompleteTime time.Time `json:"complete_time" db:"complete_time"`
}
