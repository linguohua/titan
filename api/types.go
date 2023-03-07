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

// NodeInfo Info
type NodeInfo struct {
	Base
	NodeID       string   `json:"node_id" form:"nodeId" gorm:"column:node_id;comment:;" db:"node_id"`
	UserID       string   `json:"user_id" form:"userId" gorm:"column:user_id;comment:;"`
	SnCode       string   `json:"sn_code" form:"snCode" gorm:"column:sn_code;comment:;"`
	NodeType     NodeType `json:"node_type"`
	ExternalIP   string   `json:"external_ip" form:"externalIp" gorm:"column:external_ip;comment:;"`
	InternalIP   string   `json:"internal_ip" form:"internalIp" gorm:"column:internal_ip;comment:;"`
	IPLocation   string   `json:"ip_location" form:"ipLocation" gorm:"column:ip_location;comment:;"`
	PkgLossRatio float64  `json:"pkg_loss_ratio" form:"pkgLossRatio" gorm:"column:pkg_loss_ratio;comment:;"`
	Latency      float64  `json:"latency" form:"latency" gorm:"column:latency;comment:;"`
	CPUUsage     float64  `json:"cpu_usage" form:"cpuUsage" gorm:"column:cpu_usage;comment:;"`
	MemoryUsage  float64  `json:"memory_usage" form:"memoryUsage" gorm:"column:memory_usage;comment:;"`
	Online       bool     `json:"online" form:"online" gorm:"column:online;comment:;"`

	DiskUsage       float64   `json:"disk_usage" form:"diskUsage" gorm:"column:disk_usage;comment:;" db:"disk_usage"`
	Blocks          int       `json:"blocks" form:"blockCount" gorm:"column:blocks;comment:;" db:"blocks"`
	BandwidthUp     float64   `json:"bandwidth_up" db:"bandwidth_up"`
	BandwidthDown   float64   `json:"bandwidth_down" db:"bandwidth_down"`
	NatType         string    `json:"nat_type" form:"natType" gorm:"column:nat_type;comment:;" db:"nat_type"`
	DiskSpace       float64   `json:"disk_space" form:"diskSpace" gorm:"column:disk_space;comment:;" db:"disk_space"`
	SystemVersion   string    `json:"system_version" form:"systemVersion" gorm:"column:system_version;comment:;" db:"system_version"`
	DiskType        string    `json:"disk_type" form:"diskType" gorm:"column:disk_type;comment:;" db:"disk_type"`
	IoSystem        string    `json:"io_system" form:"ioSystem" gorm:"column:io_system;comment:;" db:"io_system"`
	Latitude        float64   `json:"latitude" db:"latitude"`
	Longitude       float64   `json:"longitude" db:"longitude"`
	NodeName        string    `json:"node_name" form:"nodeName" gorm:"column:node_name;comment:;" db:"node_name"`
	Memory          float64   `json:"memory" form:"memory" gorm:"column:memory;comment:;" db:"memory"`
	CPUCores        int       `json:"cpu_cores" form:"cpuCores" gorm:"column:cpu_cores;comment:;" db:"cpu_cores"`
	ProductType     string    `json:"product_type" form:"productType" gorm:"column:product_type;comment:;" db:"product_type"`
	MacLocation     string    `json:"mac_location" form:"macLocation" gorm:"column:mac_location;comment:;" db:"mac_location"`
	OnlineTime      int       `json:"online_time" form:"OnlineTime" db:"online_time"`
	Profit          float64   `json:"profit" db:"profit"`
	DownloadTraffic float64   `json:"download_traffic" db:"download_traffic"`
	UploadTraffic   float64   `json:"upload_traffic" db:"upload_traffic"`
	DownloadBlocks  int       `json:"download_blocks" form:"downloadCount" gorm:"column:download_blocks;comment:;" db:"download_blocks"`
	PortMapping     string    `db:"port_mapping"`
	LastTime        time.Time `db:"last_time"`
	PrivateKeyStr   string    `db:"private_key"`
	Quitted         bool      `db:"quitted"`
}

// DownloadRecordInfo node download record
type DownloadRecordInfo struct {
	ID           string    `json:"-"`
	NodeID       string    `json:"node_id" db:"node_id"`
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
