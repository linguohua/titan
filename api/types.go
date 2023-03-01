package api

import (
	"time"
)

type OpenRPCDocument map[string]interface{}

type Base struct {
	ID        uint      `gorm:"primarykey"`
	CreatedAt time.Time `json:"created_at" gorm:"comment:'创建时间';type:timestamp;"`
	UpdatedAt time.Time `json:"updated_at" gorm:"comment:'更新时间';type:timestamp;"`
}

// DeviceInfo Info
type DeviceInfo struct {
	Base
	UserID           string    `json:"user_id" form:"userId" gorm:"column:user_id;comment:;"`
	SnCode           string    `json:"sn_code" form:"snCode" gorm:"column:sn_code;comment:;"`
	NodeType         NodeType  `json:"node_type" db:"node_type"`
	DeviceID         string    `json:"device_id" form:"deviceId" gorm:"column:device_id;comment:;" db:"device_id"`
	DeviceName       string    `json:"device_name" form:"deviceName" gorm:"column:device_name;comment:;" db:"device_name"`
	Operator         string    `json:"operator" form:"operator" gorm:"column:operator;comment:;" db:"operator"`
	NetworkType      string    `json:"network_type" form:"networkType" gorm:"column:network_type;comment:;" db:"network_type"`
	SystemVersion    string    `json:"system_version" form:"systemVersion" gorm:"column:system_version;comment:;" db:"system_version"`
	ProductType      string    `json:"product_type" form:"productType" gorm:"column:product_type;comment:;" db:"product_ype"`
	NetworkInfo      string    `json:"network_info" form:"networkInfo" gorm:"column:network_info;comment:;" db:"network_info"`
	ExternalIP       string    `json:"external_ip" form:"externalIp" gorm:"column:external_ip;comment:;" db:"external_ip"`
	InternalIP       string    `json:"internal_ip" form:"internalIp" gorm:"column:internal_ip;comment:;" db:"internal_ip"`
	IPLocation       string    `json:"ip_location" form:"ipLocation" gorm:"column:ip_location;comment:;" db:"ip_location"`
	MacLocation      string    `json:"mac_location" form:"macLocation" gorm:"column:mac_location;comment:;" db:"mac_location"`
	NatType          string    `json:"nat_type" form:"natType" gorm:"column:nat_type;comment:;" db:"nat_type"`
	Upnp             string    `json:"upnp" form:"upnp" gorm:"column:upnp;comment:;" db:"upnp"`
	PkgLossRatio     float64   `json:"pkg_loss_ratio" form:"pkgLossRatio" gorm:"column:pkg_loss_ratio;comment:;" db:"pkg_loss_ratio"`
	Latency          float64   `json:"latency" form:"latency" gorm:"column:latency;comment:;" db:"latency"`
	CPUUsage         float64   `json:"cpu_usage" form:"cpuUsage" gorm:"column:cpu_usage;comment:;" db:"cpu_usage"`
	CPUCores         int       `json:"cpu_cores" form:"cpuCores" gorm:"column:cpu_cores;comment:;" db:"cpu_cores"`
	MemoryUsage      float64   `json:"memory_usage" form:"memoryUsage" gorm:"column:memory_usage;comment:;" db:"memory_usage"`
	Memory           float64   `json:"memory" form:"memory" gorm:"column:memory;comment:;" db:"memory"`
	DiskUsage        float64   `json:"disk_usage" form:"diskUsage" gorm:"column:disk_usage;comment:;" db:"disk_usage"`
	DiskSpace        float64   `json:"disk_space" form:"diskSpace" gorm:"column:disk_space;comment:;" db:"disk_space"`
	DiskType         string    `json:"disk_type" form:"diskType" gorm:"column:disk_type;comment:;" db:"disk_type"`
	DeviceStatus     string    `json:"device_status" form:"deviceStatus" gorm:"column:device_status;comment:;" db:"device_status"`
	WorkStatus       string    `json:"work_status" form:"workStatus" gorm:"column:work_status;comment:;" db:"work_status"`
	IoSystem         string    `json:"io_system" form:"ioSystem" gorm:"column:io_system;comment:;" db:"io_system"`
	NatRatio         float64   `json:"nat_ratio" form:"nat_ratio" gorm:"column:nat_ratio;comment:;" db:"nat_ratio"`
	OnlineTime       int       `json:"online_time" form:"OnlineTime" db:"online_time"`
	CumulativeProfit float64   `json:"cumulative_profit" db:"cumulative_profit"`
	BandwidthUp      float64   `json:"bandwidth_up" db:"bandwidth_up"`
	BandwidthDown    float64   `json:"bandwidth_down" db:"bandwidth_down"`
	TotalDownload    float64   `json:"total_download" db:"total_townload"`
	TotalUpload      float64   `json:"total_upload" db:"total_upload"`
	DownloadCount    int       `json:"download_count" form:"downloadCount" gorm:"column:download_count;comment:;" db:"download_count"`
	BlockCount       int       `json:"block_count" form:"blockCount" gorm:"column:block_count;comment:;" db:"block_count"`
	Latitude         float64   `json:"latitude" db:"latitude"`
	Longitude        float64   `json:"longitude" db:"longitude"`
	Port             string    `db:"port"`
	LastTime         time.Time `db:"last_time"`
	PrivateKeyStr    string    `db:"private_key"`
	Quitted          bool      `db:"quitted"`
}

type BlockDownloadInfo struct {
	ID           string    `json:"-"`
	DeviceID     string    `json:"device_id" db:"device_id"`
	BlockCID     string    `json:"block_cid" db:"block_cid"`
	CarfileCID   string    `json:"carfile_cid" db:"carfile_cid"`
	BlockSize    int       `json:"block_size" db:"block_size"`
	Speed        int64     `json:"speed" db:"speed"`
	Reward       int64     `json:"reward" db:"reward"`
	Status       int       `json:"status" db:"status"`
	FailedReason string    `json:"failed_reason" db:"failed_reason"`
	ClientIP     string    `json:"client_ip" db:"client_ip"`
	CreatedTime  time.Time `json:"created_time" db:"created_time"`
	CompleteTime time.Time `json:"complete_time" db:"complete_time"`
}
